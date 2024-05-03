"""Creates a vector embedding based on a string attribute."""

import numpy as np
from tqdm import tqdm
from . import util

op = util.Op()
attr = op.input("attr")
method = op.params["method"]
model_name = op.params["model_name"].strip()


class Embedder:
  def run(self):
    batch_size = op.params["batch_size"].strip()
    batch_size = int(batch_size) if batch_size else self.DEFAULT_BATCH_SIZE
    strings = attr.tolist()
    batches = [
        strings[i: i + batch_size] for i in range(0, len(strings), batch_size)
    ]
    embeddings = []
    for strings in tqdm(batches, desc="Computing embeddings"):
      embeddings.append(self.embed(strings))
    if not embeddings:
      return embeddings
    elif isinstance(embeddings[0], list):
      # Flatten list.
      return [i for j in embeddings for i in j]
    else:
      # Concatenate Numpy/PyTorch arrays.
      return np.concatenate(embeddings)


class OpenAIEmbedder(Embedder):
  DEFAULT_BATCH_SIZE = 500

  def __init__(self):
    import openai

    self.client = openai.OpenAI()

  def embed(self, strings):
    numbered = [(i, a) for i, a in enumerate(strings) if a]
    numbers, strings = zip(*numbered)
    m = model_name or "text-embedding-3-small"
    result = self.client.embeddings.create(model=m, input=strings).data
    result = dict(zip(numbers, result))
    return [
        np.array(result[i].embedding) if i in result else None
        for i in range(len(strings))
    ]


class SentenceTransformersEmbedder(Embedder):
  DEFAULT_BATCH_SIZE = 64

  def __init__(self):
    from sentence_transformers import SentenceTransformer
    import torch

    torch.set_default_device("cuda" if torch.cuda.is_available() else "cpu")
    m = model_name or "nomic-ai/nomic-embed-text-v1"
    self.model = SentenceTransformer(m, trust_remote_code=True)
    self.model.eval()

  def embed(self, strings):
    import torch

    with torch.no_grad():
      return self.model.encode(strings)


class AnglEEmbedder(Embedder):
  DEFAULT_BATCH_SIZE = 64

  def __init__(self):
    from angle_emb import AnglE
    import torch

    torch.set_default_device("cuda" if torch.cuda.is_available() else "cpu")
    m = model_name or "WhereIsAI/UAE-Large-V1"
    self.angle = AnglE.from_pretrained(m, pooling_strategy="cls")

  def embed(self, strings):
    import torch

    with torch.no_grad():
      return self.angle.encode(strings, to_numpy=True)


class CausalTransformerEmbedder(Embedder):
  DEFAULT_BATCH_SIZE = 64

  def __init__(self):
    from transformers import AutoModelForCausalLM, AutoTokenizer
    import torch

    torch.set_default_device("cuda" if torch.cuda.is_available() else "cpu")
    m = model_name or "microsoft/DialoGPT-small"
    self.tokenizer = AutoTokenizer.from_pretrained(m)
    self.model = AutoModelForCausalLM.from_pretrained(m)
    self.model.eval()

  def embed(self, strings):
    import torch

    # Make echo embeddings. https://arxiv.org/abs/2402.15449
    # We repeat the tokens and mean pool the embeddings of the second copy.
    tokenized = self.tokenizer(strings)
    lengths = [len(i) for i in tokenized["input_ids"]]
    max_length = max(lengths)
    pad = self.tokenizer.pad_token_id or self.tokenizer.eos_token_id
    repeated = torch.tensor(
        [i * 2 + [pad, pad] * (max_length - len(i)) for i in tokenized["input_ids"]]
    )
    lengths = torch.tensor(lengths).unsqueeze(1)
    mask = torch.arange(max_length * 2).expand(len(lengths), max_length * 2)
    mask = (mask >= lengths) & (mask < lengths * 2)  # Just the second copy.
    with torch.no_grad():
      res = self.model(input_ids=repeated, output_hidden_states=True)
    h = res.hidden_states[-1]  # Take the last layer's output.
    masked = h * mask.unsqueeze(2).float()
    e = masked.sum(1) / lengths
    e[e != e] = 0  # Replace NaNs (for zero-length strings) with 0.
    return e.detach().cpu().numpy()


if method == "OpenAI":
  embeddings = OpenAIEmbedder().run()
elif method == "SentenceTransformers":
  embeddings = SentenceTransformersEmbedder().run()
elif method == "AnglE":
  embeddings = AnglEEmbedder().run()
elif method == "Causal Transformer":
  embeddings = CausalTransformerEmbedder().run()
op.output("attr", embeddings, type=util.DoubleVectorAttribute)
