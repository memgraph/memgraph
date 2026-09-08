import os


def encode_chunk(visible_gpu: int | None, model_id: str, texts: list[str], batch_size: int):
    """
    Runs in a spawned process. Returns (count, embeddings_as_list)
    """
    # Set device visibility BEFORE importing torch/transformers
    if visible_gpu is None:
        os.environ["CUDA_VISIBLE_DEVICES"] = ""
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(visible_gpu)

    os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
    os.environ.setdefault("TRANSFORMERS_NO_TORCHVISION", "1")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

    import torch
    from sentence_transformers import SentenceTransformer

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = SentenceTransformer(model_id, device=device)

    if not texts:
        return 0, []

    embs = model.encode(
        texts,
        batch_size=min(batch_size, len(texts)),
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return len(texts), embs.tolist()
