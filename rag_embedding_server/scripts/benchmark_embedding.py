import asyncio
import aiohttp
import uuid
import time
import argparse

def generate_dummy_text(length: int) -> str:
    return " ".join([
        "test"
    ] * length)

def generate_payload(batch_size: int):
    dummy_text = generate_dummy_text(5)
    return [
        {
            "chunk_id": str(uuid.uuid4()),
            "item_sequence": "200101234",
            "name": "test",
            "contents": dummy_text,
            "meta": {
                "source_ref": "TEST_SOURCE",
                "lang": "ko"
            }
        }
        for i in range(batch_size)
    ]

async def send_request(session, endpoint, payload, idx):
    start = time.time()
    try:
        async with session.post(endpoint, json=payload) as resp:
            await resp.text()
            elapsed = time.time() - start
            print(f"[Iteration {idx}] ✅ Duration: {elapsed:.3f}s, contents_size: {len(payload[0]['contents'])}")
    except Exception as e:
        print(f"[Iteration {idx}] ❌ Error: {e}")

async def run_benchmark(batch_size: int, iterations: int, endpoint: str):
    print(f"\n🚀 Running benchmark for batch size: {batch_size}")
    payloads = [generate_payload(batch_size) for _ in range(iterations)]

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[
            send_request(session, endpoint, payloads[i], i + 1)
            for i in range(iterations)
        ])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark RAG embedding throughput (async)")
    parser.add_argument("batch_size", type=int, help="Number of chunks per request")
    parser.add_argument("--iterations", type=int, default=10, help="Number of concurrent requests")
    parser.add_argument("--endpoint", type=str, default="http://localhost:8000/test/embed", help="Target endpoint URL")
    args = parser.parse_args()

    asyncio.run(run_benchmark(args.batch_size, args.iterations, args.endpoint))