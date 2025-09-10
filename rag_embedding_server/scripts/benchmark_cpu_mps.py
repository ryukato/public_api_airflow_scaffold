import time
import torch
from sentence_transformers import SentenceTransformer

TEXT = [
    "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest"
] * 10  # 10배 복제하여 처리 시간 확보

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

def benchmark(device_str):
    print(f"\n>>> Running on {device_str.upper()}")

    device = torch.device(device_str)
    model = SentenceTransformer(MODEL_NAME, device=device)

    start = time.time()
    _ = model.encode(TEXT, batch_size=8, normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=True)
    elapsed = time.time() - start

    print(f"Elapsed time ({device_str}): {elapsed:.2f} sec")

if __name__ == "__main__":
    # Run on CPU
    benchmark("cpu")

    # Run on MPS (only works on macOS with Apple Silicon + MPS enabled)
    if torch.backends.mps.is_available():
        benchmark("mps")
    else:
        print("\n[MPS not available]")