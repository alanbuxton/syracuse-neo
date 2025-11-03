from sentence_transformers import SentenceTransformer
from django.conf import settings

MODEL=SentenceTransformer(settings.EMBEDDINGS_MODEL)
