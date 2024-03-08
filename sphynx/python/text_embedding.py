"""Creates a vector embedding based on a string attribute."""

import numpy as np
import torch
from . import util

device = 'cuda' if torch.cuda.is_available() else 'cpu'
torch.set_default_device(device)
op = util.Op()
attr = op.input("attr")
method = op.params["method"]
model_name = op.params["model_name"].strip()
if method == "OpenAI":
    import openai

    model_name = model_name or "text-embedding-3-small"
    client = openai.OpenAI()
    numbered = [(i, a) for i, a in enumerate(attr) if a]
    numbers, strings = zip(*numbered)
    result = client.embeddings.create(model=model_name, input=strings).data
    result = dict(zip(numbers, result))
    embedding = [
        np.array(result[i].embedding) if i in result else None for i in range(len(attr))
    ]
elif method == "SentenceTransformers":
    from sentence_transformers import SentenceTransformer

    model_name = model_name or "nomic-ai/nomic-embed-text-v1"
    model = SentenceTransformer(model_name, trust_remote_code=True)
    embedding = model.encode(attr.tolist())
elif method == "AnglE":
    from angle_emb import AnglE

    model_name = model_name or "WhereIsAI/UAE-Large-V1"
    angle = AnglE.from_pretrained(model_name, pooling_strategy="cls")
    embedding = angle.encode(attr.tolist(), to_numpy=True)
op.output("attr", embedding, type=util.DoubleVectorAttribute)
