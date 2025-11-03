from sklearn.cluster import AgglomerativeClustering
import numpy as np 
from collections import Counter 
import logging 
logger = logging.getLogger(__name__)

from integration.embeddings_model import MODEL


def top_central_sentences_from_clusters(corpus, distance_threshold, keep, model=MODEL):
    centroids = cluster_sentences(corpus, model, distance_threshold=distance_threshold)
    tops = top_n_pct(centroids, keep)
    return tops

def cluster_sentences(corpus, model, distance_threshold=0.9):
    # from https://github.com/huggingface/sentence-transformers/blob/master/examples/sentence_transformer/applications/clustering/agglomerative.py
    corpus_embeddings = model.encode(corpus)
    # Some models don't automatically normalize the embeddings, in which case you should normalize the embeddings:
    # corpus_embeddings = corpus_embeddings / np.linalg.norm(corpus_embeddings, axis=1, keepdims=True)
    clustering_model = AgglomerativeClustering(
        n_clusters=None, distance_threshold=distance_threshold,
        linkage='average')
    cluster_labels = clustering_model.fit_predict(corpus_embeddings)

    centroids = {}
    for cluster_id in np.unique(cluster_labels):
        cluster_mask = cluster_labels == cluster_id
        cluster_embeddings = corpus_embeddings[cluster_mask]
        cluster_sentences = [corpus[i] for i, mask in enumerate(cluster_mask) if mask] 
        logger.debug(f"{cluster_id} has {len(cluster_sentences)} sents: {cluster_sentences}")
        centroid = cluster_embeddings.mean(axis=0)
        distances = np.linalg.norm(cluster_embeddings - centroid, axis=1)
        closest_idx = distances.argmin()
        centroids[cluster_sentences[closest_idx]] =  len(cluster_sentences)

    return centroids

def top_n_pct(d: dict, max_proportion=0.8):
    counter = Counter(d)
    total = sum(counter.values()) 
    threshold = total * max_proportion 

    top_items = []
    cumulative = 0
    for item, count in counter.most_common():
        top_items.append(item)
        cumulative += count
        if cumulative >= threshold:
            break    
    return top_items
