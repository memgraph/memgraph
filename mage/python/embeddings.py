import multiprocessing as mp
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List

import huggingface_hub  # noqa: F401
import mgp

# We need to import huggingface_hub, otherwise sentence_transformers will fail to load the model.

sys.path.append(os.path.join(os.path.dirname(__file__), "embed_worker"))
logger: mgp.Logger = mgp.Logger()

os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
os.environ.setdefault("TRANSFORMERS_NO_TORCHVISION", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")


def build_texts(vertices, excluded_properties):
    logger.debug(f"excluded_properties: {excluded_properties}")
    out = []
    for vertex in vertices:
        txt = (
            " ".join(lbl.name for lbl in vertex.labels)
            + " "
            + " ".join(f"{key}: {val}" for key, val in vertex.properties.items() if key not in excluded_properties)
        )
        out.append(txt)
    logger.debug(f"text to calc embedding: {out}")
    return out


def split_slices(n_items: int, n_parts: int):
    base, rem = divmod(n_items, n_parts)
    start = 0
    slices = []
    for i in range(n_parts):
        end = start + base + (1 if i < rem else 0)
        slices.append((start, end))
        start = end
    return slices


def get_visible_gpus():
    # Avoid creating a CUDA context in the parent if possible
    try:
        import subprocess

        out = subprocess.check_output(["nvidia-smi", "--query-gpu=index", "--format=csv,noheader"], text=True)
        return [int(x) for x in out.strip().splitlines() if x.strip()]
    except Exception:
        try:
            import torch

            return list(range(torch.cuda.device_count())) if torch.cuda.is_available() else []
        except Exception:
            return []


def select_device(device: mgp.Any):  # noqa: C901
    """
    Determine and validate which CUDA device(s) can be used for the given target.

    Allowed inputs:
    - int: a single GPU index
    - list[int]: a list of GPU indices
    - list[str]: a list of GPU names
    - str: a single GPU name (e.g. "cuda:0", "cuda:1", "cuda:2", etc.), "cuda", "all" or "cpu"

    Returns:
    - list[int]: List of valid GPU indices to use
    - None: If "cpu" is specified or no valid GPUs found
    """
    # Get available GPUs
    available_gpus = get_visible_gpus()

    if isinstance(device, tuple):
        device = list(device)

    # Check if input is "cpu" when no CUDA devices are available
    if not available_gpus:
        if (isinstance(device, str) and device.lower() == "cpu") or device is None:
            logger.info("No CUDA devices available, using CPU")
            return None
        else:
            raise RuntimeError("No CUDA devices available and device is not 'cpu'")
    elif device is None:
        # If GPU is available but not explicitly requested, use the first available one
        return [available_gpus[0]]

    # Handle different input types
    if isinstance(device, int):
        # Single GPU index
        if device < 0:
            raise ValueError(f"GPU index must be non-negative, got {device}")
        if device not in available_gpus:
            raise ValueError(f"GPU {device} not available. Available GPUs: {available_gpus}")
        return [device]

    elif isinstance(device, str):
        # String input - could be "cpu" or "cuda:X"
        if device.lower() == "cpu":
            return None

        if device.lower() in ["all", "cuda"]:
            return available_gpus

        if device.startswith("cuda:"):
            try:
                gpu_index = int(device.split(":")[1])
                if gpu_index < 0:
                    raise ValueError(f"GPU index must be non-negative, got {gpu_index}")
                if gpu_index not in available_gpus:
                    raise ValueError(f"GPU {gpu_index} not available. Available GPUs: {available_gpus}")
                return [gpu_index]
            except (ValueError, IndexError) as e:
                raise ValueError(
                    f"Invalid CUDA device format '{device}'. Expected format: 'cuda:X' where X is a number"
                ) from e
        else:
            raise ValueError(f"Invalid device string '{device}'. Expected 'cpu' or 'cuda:X'")

    elif isinstance(device, list):
        if not device:
            raise ValueError("Empty device list provided")

        # Check if it's a list of integers or strings
        if all(isinstance(x, int) for x in device):
            # List of GPU indices
            for gpu_idx in device:
                if gpu_idx < 0:
                    raise ValueError(f"GPU index must be non-negative, got {gpu_idx}")
                if gpu_idx not in available_gpus:
                    raise ValueError(f"GPU {gpu_idx} not available. Available GPUs: {available_gpus}")
            return device.copy()

        elif all(isinstance(x, str) for x in device):
            # List of GPU names/strings
            gpu_indices = []
            for device_str in device:
                if device_str.lower() == "cpu":
                    logger.warning("'cpu' found in device list, ignoring")
                    continue

                if device_str.startswith("cuda:"):
                    try:
                        gpu_index = int(device_str.split(":")[1])
                        if gpu_index < 0:
                            raise ValueError(f"GPU index must be non-negative, got {gpu_index}")
                        if gpu_index not in available_gpus:
                            raise ValueError(f"GPU {gpu_index} not available. Available GPUs: {available_gpus}")
                        gpu_indices.append(gpu_index)
                    except (ValueError, IndexError) as e:
                        raise ValueError(
                            f"Invalid CUDA device format '{device_str}'. Expected format: 'cuda:X' where X is a number"
                        ) from e
                else:
                    raise ValueError(f"Invalid device string '{device_str}'. Expected 'cpu' or 'cuda:X'")

            if not gpu_indices:
                logger.warning("No valid GPU devices found in list, falling back to CPU")
                return None

            return gpu_indices
        else:
            raise ValueError("Device list must contain only integers or strings, not mixed types")
    else:
        raise TypeError(f"Invalid device type {type(device)}. Expected int, str, or list of int/str")


def cpu_compute(
    input_items: mgp.Any,  # Can be vertices or strings
    embedding_property: str = "embedding",
    excluded_properties: mgp.Nullable[
        mgp.List[str]  # NOTE: It's a list because Memgraph query modules do NOT support sets yet.
    ] = None,  # https://dev.to/ytskk/dont-use-mutable-default-arguments-in-python-56f4
    model_name: str = "all-MiniLM-L6-v2",
    batch_size: int = 2000,
    return_embeddings: bool = False,
    dimension: int = None,
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=int):
    import transformers  # noqa: F401
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name, device="cpu")
    vertex_input = isinstance(embedding_property, str)
    if vertex_input:
        texts = build_texts(input_items, excluded_properties)
    else:
        texts = input_items

    n = len(input_items)
    embs = model.encode(
        texts,
        batch_size=min(batch_size, n),
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    embeddings_list = embs.tolist()
    if vertex_input:
        for v, e in zip(input_items, embeddings_list):
            v.properties[embedding_property] = e

    logger.info(f"Processed {n} items on CPU.")
    return return_data(
        input_items if vertex_input else embeddings_list,
        embedding_property_name=embedding_property if vertex_input else None,
        return_embeddings=return_embeddings,
        success=True,
        dimension=dimension,
    )


def single_gpu_compute(
    input_items: mgp.Any,  # Can be vertices or strings
    embedding_property: str = "embedding",
    excluded_properties: mgp.Nullable[
        mgp.List[str]  # NOTE: It's a list because Memgraph query modules do NOT support sets yet.
    ] = None,  # https://dev.to/ytskk/dont-use-mutable-default-arguments-in-python-56f4
    model_name: str = "all-MiniLM-L6-v2",
    batch_size: int = 2000,
    device: int = 0,
    return_embeddings: bool = False,
    dimension: int = None,
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=int):
    import gc

    import torch
    import transformers  # noqa: F401
    from sentence_transformers import SentenceTransformer

    vertex_input = isinstance(embedding_property, str)
    model = None
    allocated_memory = 0
    try:
        try:
            model = SentenceTransformer(model_name, device=f"cuda:{device}")
            allocated_memory = torch.cuda.memory_allocated()
            logger.info(f"Allocated memory: {allocated_memory / 1024 / 1024:.2f} MB")
        except Exception as e:
            logger.error(f"Failed to load model {model_name}: {e}")
            return return_data(
                input_items if vertex_input else None,
                embedding_property_name=embedding_property if vertex_input else None,
                return_embeddings=return_embeddings,
                success=False,
                dimension=dimension,
            )
        item_iter = iter(input_items)
        n = len(input_items)
        all_embeddings = []  # only used for string inputs
        for i in range(0, n, batch_size):
            batch = []
            for _ in range(batch_size):
                try:
                    batch.append(next(item_iter))
                except StopIteration:
                    break
            if vertex_input:
                batch_texts = build_texts(batch, excluded_properties)
            else:
                batch_texts = batch
            embs = model.encode(
                batch_texts,
                batch_size=batch_size,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            embeddings_list = embs.tolist()
            if vertex_input:
                for v, e in zip(batch, embeddings_list):
                    v.properties[embedding_property] = e
            else:
                all_embeddings.extend(embeddings_list)

        logger.info(f"Processed {len(input_items)} items on GPU {device}.")
        return return_data(
            input_items if vertex_input else all_embeddings,
            embedding_property_name=embedding_property if vertex_input else None,
            return_embeddings=return_embeddings,
            success=True,
            dimension=dimension,
        )

    finally:
        # TODO(matt): figure out why destructor for the model is not called...
        logger.info("Freeing GPU memory...")
        if model is not None:
            model.to("cpu")
            del model
            freed_memory = allocated_memory - torch.cuda.memory_allocated()
        # Force garbage collection
        gc.collect()

        # Clear PyTorch cache
        torch.cuda.empty_cache()
        torch.cuda.synchronize()

        logger.info(f"GPU {device} Freed memory: {freed_memory / 1024 / 1024:.2f} MB")


def multi_gpu_compute(
    input_items: mgp.Any,  # Can be vertices or strings
    embedding_property: str = "embedding",
    excluded_properties: mgp.Nullable[
        mgp.List[str]  # NOTE: It's a list because Memgraph query modules do NOT support sets yet.
    ] = None,  # https://dev.to/ytskk/dont-use-mutable-default-arguments-in-python-56f4
    model_name: str = "all-MiniLM-L6-v2",
    batch_size: int = 2000,
    chunk_size: int = 48,
    gpus: List[int] = [0],
    return_embeddings: bool = False,
    dimension: int = None,
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=int):
    vertex_input = isinstance(embedding_property, str)

    try:
        import embed_worker
    except Exception as e:
        logger.error(f"Failed to import worker module: {e}")
        return return_data(
            input_items if vertex_input else None,
            embedding_property_name=embedding_property,
            return_embeddings=return_embeddings,
            success=False,
            dimension=dimension,
        )

    n = len(input_items)

    # Multi-GPU via spawn - process in chunks to avoid memory issues
    # We spawn a worker process for each GPU every time we process a chunk.
    # every time that happens, it takes about 8-10s to import libraries and load the model.
    # `chunk_size` shoiuld be tweaked to minimize the number of times we spawn a worker process,
    # while avoiding OOM.
    chunk_size = min(batch_size * chunk_size, n)
    total_processed = 0

    # Create an iterator from the input items
    item_iter = iter(input_items)

    all_embeddings = []  # only used for string inputs
    for chunk_start in range(0, n, chunk_size):
        chunk_end = min(chunk_start + chunk_size, n)

        # Collect only the input items for this chunk
        chunk_items = []
        for _ in range(chunk_end - chunk_start):
            try:
                chunk_items.append(next(item_iter))
            except StopIteration:
                break

        if not chunk_items:
            break

        if vertex_input:
            chunk_texts = build_texts(chunk_items, excluded_properties)
        else:
            chunk_texts = chunk_items

        # Split this chunk across GPUs
        chunk_slices = split_slices(len(chunk_texts), len(gpus))
        tasks = []
        for gpu, (a, b) in zip(gpus, chunk_slices):
            if a < b:
                tasks.append((gpu, model_name, chunk_texts[a:b], batch_size, chunk_start + a, chunk_start + b))

        # Process this chunk
        chunk_results = []
        chunk_total = 0

        mp.set_executable("/usr/bin/python3")
        ctx_spawn = mp.get_context("spawn")
        with ProcessPoolExecutor(max_workers=len(tasks), mp_context=ctx_spawn) as ex:
            fut2info = {
                ex.submit(embed_worker.encode_chunk, t[0], t[1], t[2], t[3]): (
                    t[0],
                    t[4],
                    t[5],
                )
                for t in tasks
            }
            for fut in as_completed(fut2info):
                gpu, a, b = fut2info[fut]
                try:
                    count, embs = fut.result()
                    if count != (b - a) or len(embs) != (b - a):
                        logger.error(f"GPU {gpu} returned mismatched count {count} for slice [{a}:{b}]")
                        continue
                    chunk_results.append((a, b, embs))
                    chunk_total += count
                except Exception as e:
                    logger.error(f"Worker on GPU {gpu} failed: {e}")

        # Write back results for this chunk
        for a, b, embs in chunk_results:
            if vertex_input:
                for i, e in enumerate(embs, start=a):
                    chunk_items[i - chunk_start].properties[embedding_property] = e
            else:
                all_embeddings.extend(embs)
        total_processed += chunk_total

    logger.info(f"Successfully processed {total_processed}/{n} items across {len(gpus)} GPU(s).")
    success_flag = total_processed == n
    return return_data(
        input_items if vertex_input else all_embeddings,
        embedding_property_name=embedding_property,
        return_embeddings=return_embeddings,
        success=success_flag,
        dimension=dimension,
    )


def return_data(
    input_items: mgp.Any,
    embedding_property_name: mgp.Nullable[str] = "embedding",
    return_embeddings: bool = False,
    success: bool = True,
    dimension: int = None,
) -> mgp.Any:
    """
    Return embeddings and success status.

    For vertices with embeddings stored as properties:
      - Set return_embeddings=True to return embeddings from the property
      - Returns embeddings only if requested and operation succeeded

    For strings/computed embeddings (embedding_property_name is None):
      - Always returns the embeddings if operation succeeded
    """
    embeddings = None

    # Case 1: Items are embeddings themselves (strings from embed function)
    if embedding_property_name is None:
        if success:
            embeddings = input_items

    # Case 2: Extract embeddings from vertex properties
    elif return_embeddings and success:
        embeddings = [v.properties[embedding_property_name] for v in input_items]

    return mgp.Record(success=success, embeddings=embeddings, dimension=dimension)


def validate_configuration(configuration: mgp.Map):
    default_configuration = {
        "embedding_property": "embedding",
        "excluded_properties": ["embedding"],
        "model_name": "all-MiniLM-L6-v2",
        "batch_size": 2000,
        "chunk_size": 48,
        "device": None,
        "return_embeddings": False,
    }
    configuration = {**default_configuration, **configuration}

    if not configuration.get("excluded_properties"):
        configuration["excluded_properties"] = configuration["embedding_property"]
    if (
        configuration["embedding_property"] is not None
        and not configuration["embedding_property"] in configuration["excluded_properties"]
    ):
        configuration["excluded_properties"].append(configuration["embedding_property"])

    logger.debug(f"Using embedding configuration: {configuration}")

    return configuration


def compute_embeddings(
    input_items: mgp.Any,
    configuration: mgp.Map,
) -> mgp.Any:
    dimension = get_model_info(configuration).get("dimension", None)
    if dimension is None:
        logger.warning("Failed to get model dimension.")

    try:
        n = len(input_items)
        if n == 0:
            logger.info("No vertices to process.")
            return return_data(
                input_items,
                configuration["embedding_property"],
                configuration["return_embeddings"],
                True,
                dimension=dimension,
            )

        # Validate and select target GPU(s)
        try:
            gpus = select_device(configuration["device"])
        except (ValueError, TypeError, RuntimeError) as e:
            logger.error(f"Invalid device parameter: {e}")
            return return_data(
                input_items,
                configuration["embedding_property"],
                configuration["return_embeddings"],
                False,
                dimension=dimension,
            )

        logger.info(f"Selected {len(gpus) if gpus else 0} GPU(s): {gpus}")

        if not gpus:
            try:
                return cpu_compute(
                    input_items,
                    configuration["embedding_property"],
                    configuration["excluded_properties"],
                    configuration["model_name"],
                    configuration["batch_size"],
                    configuration["return_embeddings"],
                    dimension=dimension,
                )
            except Exception as e:
                logger.error(f"CPU path failed: {e}")
                return return_data(
                    input_items,
                    configuration["embedding_property"],
                    configuration["return_embeddings"],
                    False,
                    dimension=dimension,
                )

        if len(gpus) == 1:
            try:
                return single_gpu_compute(
                    input_items,
                    configuration["embedding_property"],
                    configuration["excluded_properties"],
                    configuration["model_name"],
                    configuration["batch_size"],
                    gpus[0],
                    configuration["return_embeddings"],
                    dimension=dimension,
                )
            except Exception as e:
                logger.error(f"Single GPU path failed: {e}")
                return return_data(
                    input_items,
                    configuration["embedding_property"],
                    configuration["return_embeddings"],
                    False,
                    dimension=dimension,
                )

        if len(gpus) > 1:
            try:
                return multi_gpu_compute(
                    input_items,
                    configuration["embedding_property"],
                    configuration["excluded_properties"],
                    configuration["model_name"],
                    configuration["batch_size"],
                    configuration["chunk_size"],
                    gpus,
                    configuration["return_embeddings"],
                    dimension=dimension,
                )
            except Exception as e:
                logger.error(f"Multi GPU path failed: {e}")
                return return_data(
                    input_items,
                    configuration["embedding_property"],
                    configuration["return_embeddings"],
                    False,
                    dimension=dimension,
                )
    except Exception as e:
        logger.error(f"Failed to compute embeddings: {e}")
        return return_data(
            input_items,
            configuration["embedding_property"],
            configuration["return_embeddings"],
            False,
            dimension=dimension,
        )


def get_model_info(configuration: mgp.Map):
    import transformers  # noqa: F401
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(configuration["model_name"], device="cpu")

    info = {
        "model_name": configuration["model_name"],
        "dimension": model.get_sentence_embedding_dimension(),
        "max_sequence_length": model.get_max_seq_length(),
    }

    return info


@mgp.read_proc
def model_info(
    configuration: mgp.Map = {},
) -> mgp.Record(info=mgp.Map):
    configuration = validate_configuration(configuration)

    info = get_model_info(configuration)
    return mgp.Record(info=info)


@mgp.write_proc
def node_sentence(
    ctx: mgp.ProcCtx,
    input_nodes: mgp.Nullable[mgp.List[mgp.Vertex]] = None,
    configuration: mgp.Map = {},
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=int):
    logger.info(f"compute_embeddings: starting (py_exec={sys.executable}, py_ver={sys.version.split()[0]})")

    configuration = validate_configuration(configuration)
    if input_nodes:
        vertices = input_nodes
    else:
        vertices = ctx.graph.vertices

    return compute_embeddings(vertices, configuration)


@mgp.read_proc
def text(
    ctx: mgp.ProcCtx,
    input_strings: mgp.List[str],
    configuration: mgp.Map = {},
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=int):
    logger.info(f"embed: starting (py_exec={sys.executable}, py_ver={sys.version.split()[0]})")

    # hard code embedding_property to None for string input
    configuration["embedding_property"] = None
    configuration = validate_configuration(configuration)

    return compute_embeddings(input_strings, configuration)
