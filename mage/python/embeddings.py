import multiprocessing as mp
import os
import sys
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import List

import huggingface_hub  # noqa: F401
import litellm
import mgp

# Suppress LiteLLM's "Provider List: ..." banner that fires on every
# get_llm_provider() miss — we probe deliberately for local names.
litellm.suppress_debug_info = True

# We need to import huggingface_hub, otherwise sentence_transformers will fail to load the model.

sys.path.append(os.path.join(os.path.dirname(__file__), "embed_worker"))
logger: mgp.Logger = mgp.Logger()

os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
os.environ.setdefault("TRANSFORMERS_NO_TORCHVISION", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

_model_cache = {}
_model_lock = threading.Lock()


def _get_or_load_model(model_name: str, device: str = "cpu"):
    """Thread-safe model loading with caching."""
    key = (model_name, device)
    if key in _model_cache:
        return _model_cache[key]
    with _model_lock:
        # Double-check after acquiring lock
        if key in _model_cache:
            return _model_cache[key]
        import transformers  # noqa: F401
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(model_name, device=device)
        _model_cache[key] = model
        return model


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
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=mgp.Nullable[int]):
    model = _get_or_load_model(model_name, "cpu")
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
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=mgp.Nullable[int]):
    vertex_input = isinstance(embedding_property, str)
    try:
        model = _get_or_load_model(model_name, f"cuda:{device}")
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
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=mgp.Nullable[int]):
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
    # `chunk_size` should be tweaked to minimize the number of times we spawn a worker process,
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


def resolve_remote_model(model_name):
    """Ask LiteLLM whether ``model_name`` belongs to one of its providers.

    Returns ``(model, provider, default_api_base)`` if recognized (e.g.
    ``"openai/text-embedding-3-small"``), otherwise ``None`` — in which case
    the caller should fall through to the local SentenceTransformer path.
    """
    if not isinstance(model_name, str) or not model_name:
        return None
    try:
        model, provider, _dynamic_key, default_api_base = litellm.get_llm_provider(model_name)
    except Exception:
        return None
    return model, provider, default_api_base


def default_remote_batch_size(provider):
    # Conservative defaults that stay under each provider's per-request input cap.
    return {
        "voyage": 128,
        "cohere": 96,
        "openai": 512,
        "azure": 512,
        "mistral": 512,
        "jina_ai": 512,
        "ollama": 64,
        "huggingface": 64,
        "bedrock": 64,
    }.get(provider, 64)


def l2_normalize(vec):
    s = sum(x * x for x in vec) ** 0.5
    if s == 0:
        return vec
    return [x / s for x in vec]


def remote_compute(
    input_items: mgp.Any,
    cfg: mgp.Map,
    dimension: int,
    resolved,
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=mgp.Nullable[int]):
    """LiteLLM-routed remote embedding path.

    Fans chunks across a small thread pool, preserves input order, and
    L2-normalizes client-side when ``cfg["normalize"]`` (default) so behavior
    matches the local sentence_transformers path. Raises on permanent failure
    — the caller catches and converts to ``success=False``.
    """

    _model, provider, default_api_base = resolved
    vertex_input = isinstance(cfg["embedding_property"], str)
    texts = build_texts(input_items, cfg["excluded_properties"]) if vertex_input else list(input_items)

    n = len(texts)
    if n == 0:
        return return_data(
            input_items if vertex_input else [],
            embedding_property_name=cfg["embedding_property"] if vertex_input else None,
            return_embeddings=cfg["return_embeddings"],
            success=True,
            dimension=dimension,
        )

    chunk_size = cfg["remote_batch_size"] or default_remote_batch_size(provider)
    chunks = [texts[i : i + chunk_size] for i in range(0, n, chunk_size)]

    api_base = cfg["api_base"] or default_api_base
    base_kwargs = {
        "model": cfg["model_name"],
        "num_retries": cfg["num_retries"],
        "timeout": cfg["timeout"],
    }
    if api_base:
        base_kwargs["api_base"] = api_base
    if cfg["input_type"]:
        base_kwargs["input_type"] = cfg["input_type"]
    if cfg["dimensions"]:
        base_kwargs["dimensions"] = cfg["dimensions"]

    def _call(chunk_texts):
        resp = litellm.embedding(input=chunk_texts, **base_kwargs)
        return [d["embedding"] for d in resp["data"]]

    results = [None] * len(chunks)
    with ThreadPoolExecutor(max_workers=max(1, cfg["concurrency"])) as ex:
        fut2idx = {ex.submit(_call, c): i for i, c in enumerate(chunks)}
        for fut in as_completed(fut2idx):
            results[fut2idx[fut]] = fut.result()

    flat = [e for part in results for e in part]
    if cfg["normalize"]:
        flat = [l2_normalize(e) for e in flat]

    if vertex_input:
        for v, e in zip(input_items, flat):
            v.properties[cfg["embedding_property"]] = e

    logger.info(f"Processed {n} items via LiteLLM provider '{provider}' (model={cfg['model_name']}).")
    return return_data(
        input_items if vertex_input else flat,
        embedding_property_name=cfg["embedding_property"] if vertex_input else None,
        return_embeddings=cfg["return_embeddings"],
        success=True,
        dimension=dimension or (len(flat[0]) if flat else None),
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
        # Local path
        "embedding_property": "embedding",
        "excluded_properties": ["embedding"],
        "model_name": "all-MiniLM-L6-v2",
        "batch_size": 2000,
        "chunk_size": 48,
        "device": None,
        "return_embeddings": False,
        # Remote path (via LiteLLM). These are only used when model_name resolves
        # to a LiteLLM-known provider (e.g. "openai/text-embedding-3-small").
        # API keys are NOT accepted here — LiteLLM reads canonical provider env
        # vars (OPENAI_API_KEY, COHERE_API_KEY, VOYAGE_API_KEY, ...).
        "api_base": None,
        "input_type": "document",
        "dimensions": None,
        "timeout": 60,
        "num_retries": 3,
        "normalize": True,
        "remote_batch_size": None,
        "concurrency": 4,
    }
    configuration = {**default_configuration, **configuration}

    # Ensure excluded_properties is a list (Cypher may pass it as a tuple)
    excluded = configuration.get("excluded_properties")
    if not excluded:
        configuration["excluded_properties"] = (
            [configuration["embedding_property"]] if configuration["embedding_property"] is not None else []
        )
    elif not isinstance(excluded, list):
        configuration["excluded_properties"] = list(excluded)

    excluded = configuration["excluded_properties"]
    if configuration["embedding_property"] is not None and configuration["embedding_property"] not in excluded:
        excluded.append(configuration["embedding_property"])

    # When routing remote, a local `device` setting has no meaning — warn so the
    # user knows we're ignoring it rather than silently doing the wrong thing.
    if resolve_remote_model(configuration["model_name"]) is not None and configuration["device"] is not None:
        logger.warning(
            f"'device' is ignored when model_name '{configuration['model_name']}' routes to a remote provider."
        )

    logger.debug(f"Using embedding configuration: {_redacted_config(configuration)}")

    return configuration


def _redacted_config(cfg):
    """Return a shallow-copied config with URL-embedded credentials masked.

    We don't accept secret-bearing config keys (credentials come from provider
    env vars), so the only vector left is an ``api_base`` URL of the form
    ``https://user:pass@host/...`` — scrub just those.
    """
    import re

    api_base = cfg.get("api_base")
    if not isinstance(api_base, str) or "@" not in api_base:
        return cfg
    return {**cfg, "api_base": re.sub(r"(://)[^/@\s]+:[^/@\s]+@", r"\1<redacted>@", api_base)}


def compute_embeddings(
    input_items: mgp.Any,
    configuration: mgp.Map,
) -> mgp.Any:
    try:
        n = len(input_items)

        # Route to LiteLLM if model_name names a known remote provider
        # (e.g. "openai/text-embedding-3-small", "ollama/nomic-embed-text").
        # Bare names and HF-style names like "BAAI/bge-small-en-v1.5" fall
        # through to the local SentenceTransformer path below.
        resolved = resolve_remote_model(configuration["model_name"])
        if resolved is not None:
            if n == 0:
                logger.info("No items to process.")
                return return_data(
                    input_items,
                    configuration["embedding_property"],
                    configuration["return_embeddings"],
                    True,
                    dimension=None,
                )
            try:
                # Dimension is derived from the encode response inside
                # remote_compute — no separate probe needed.
                return remote_compute(input_items, configuration, None, resolved)
            except Exception as e:
                logger.error(f"Remote path failed: {e}")
                return return_data(
                    input_items,
                    configuration["embedding_property"],
                    configuration["return_embeddings"],
                    False,
                    dimension=None,
                )

        # Local path: probe the SentenceTransformer for dimension up front.
        dimension = get_model_info(configuration).get("dimension", None)
        if dimension is None:
            logger.warning("Failed to get model dimension.")

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


_remote_info_cache = {}
_remote_info_lock = threading.Lock()


def get_model_info(configuration: mgp.Map):
    resolved = resolve_remote_model(configuration["model_name"])
    if resolved is not None:
        return _remote_model_info(configuration, resolved)

    model = _get_or_load_model(configuration["model_name"], "cpu")

    info = {
        "model_name": configuration["model_name"],
        "dimension": model.get_sentence_embedding_dimension(),
        "max_sequence_length": model.get_max_seq_length(),
    }

    return info


def _remote_model_info(configuration, resolved):
    """Probe a remote provider once to learn its embedding dimension.

    Cached per (model_name, api_base) so subsequent calls don't re-hit the API.
    """
    _model, _provider, default_api_base = resolved
    api_base = configuration.get("api_base") or default_api_base
    cache_key = (configuration["model_name"], api_base)

    with _remote_info_lock:
        cached = _remote_info_cache.get(cache_key)
    if cached is not None:
        return cached

    kwargs = {
        "model": configuration["model_name"],
        "input": ["probe"],
        "num_retries": configuration.get("num_retries", 3),
        "timeout": configuration.get("timeout", 60),
    }
    if api_base:
        kwargs["api_base"] = api_base
    if configuration.get("dimensions"):
        kwargs["dimensions"] = configuration["dimensions"]

    resp = litellm.embedding(**kwargs)
    dim = len(resp["data"][0]["embedding"])
    info = {
        "model_name": configuration["model_name"],
        "dimension": dim,
        "max_sequence_length": None,
    }

    with _remote_info_lock:
        _remote_info_cache[cache_key] = info
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
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=mgp.Nullable[int]):
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
) -> mgp.Record(success=bool, embeddings=mgp.Nullable[mgp.List[list]], dimension=mgp.Nullable[int]):
    logger.info(f"embed: starting (py_exec={sys.executable}, py_ver={sys.version.split()[0]})")

    # hard code embedding_property to None for string input
    configuration["embedding_property"] = None
    configuration = validate_configuration(configuration)

    return compute_embeddings(input_strings, configuration)
