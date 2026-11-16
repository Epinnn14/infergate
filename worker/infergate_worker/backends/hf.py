"""A real model with Hugging Face transformers (CPU or GPU).

Small instruct models (Qwen2.5-0.5B-Instruct, SmolLM2-360M-Instruct) run on a laptop CPU.
Generation runs in threads; ``capacity`` bounds how many run at once. Cancellation stops
generation through a StoppingCriteria.
"""

from __future__ import annotations

import asyncio
import threading

from . import Cancelled, Emit, Job, Result


class HFBackend:
    def __init__(self, cfg):
        self.cfg = cfg
        self.sem = asyncio.Semaphore(max(1, cfg.capacity))
        self.model = None
        self.tok = None

    async def start(self) -> None:
        await asyncio.to_thread(self._load)

    def _load(self) -> None:
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self.tok = AutoTokenizer.from_pretrained(self.cfg.hf_model)
        dtype = torch.float16 if self.cfg.hf_device.startswith("cuda") else torch.float32
        self.model = AutoModelForCausalLM.from_pretrained(self.cfg.hf_model, torch_dtype=dtype).to(self.cfg.hf_device)
        self.model.eval()

    def stats(self) -> dict:
        return {"model": self.cfg.hf_model}

    async def generate(self, job: Job, emit: Emit, cancelled: Cancelled) -> Result:
        from transformers import StoppingCriteria, StoppingCriteriaList, TextIteratorStreamer

        stop = threading.Event()

        class _Stop(StoppingCriteria):
            def __call__(self, input_ids, scores, **kwargs):
                return stop.is_set()

        async with self.sem:
            ids = self.tok.apply_chat_template(job.messages, add_generation_prompt=True, return_tensors="pt").to(self.cfg.hf_device)
            streamer = TextIteratorStreamer(self.tok, skip_prompt=True, skip_special_tokens=True)
            kwargs = dict(
                input_ids=ids,
                max_new_tokens=job.max_tokens,
                streamer=streamer,
                stopping_criteria=StoppingCriteriaList([_Stop()]),
                do_sample=job.temperature > 0,
                pad_token_id=self.tok.eos_token_id,
            )
            if job.temperature > 0:
                kwargs["temperature"] = job.temperature
            thread = threading.Thread(target=self.model.generate, kwargs=kwargs, daemon=True)
            thread.start()
            out = 0
            text = []
            reason = "stop"
            loop = asyncio.get_running_loop()
            it = iter(streamer)
            while True:
                piece = await loop.run_in_executor(None, next, it, None)
                if piece is None:
                    break
                if piece:
                    await emit(piece)
                    text.append(piece)
                    out += 1
                if out % 8 == 0 and await cancelled():
                    stop.set()
                    reason = "cancelled"
            await asyncio.to_thread(thread.join)
            # the streamer yields text pieces, not tokens: count real tokens for usage
            tokens = len(self.tok.encode("".join(text), add_special_tokens=False))
            if reason == "stop" and tokens >= job.max_tokens:
                reason = "length"
            return Result(prompt_tokens=int(ids.shape[-1]), output_tokens=tokens, finish_reason=reason)
