"""Trust cluster detection via label propagation on the follow graph."""

import re
from collections import Counter

CLUSTER_COLORS = [
    "#e74c3c", "#3498db", "#2ecc71", "#f39c12", "#9b59b6",
    "#1abc9c", "#e67e22", "#34495e", "#16a085", "#c0392b",
]

_STOPWORDS = frozenset(
    "i me my we our you your he she it they them this that a an the and but or "
    "is am are was were be been have has had do does did will would shall should "
    "can could may might must to of in for on with at by from as into about up "
    "out if not no so than too very just also how all each every both few more "
    "most other some such only own same than http https com www nostr bitcoin btc "
    "relay im de el la le en et un une des les".split()
)

_WORD_RE = re.compile(r"[a-z]{3,20}")


def detect_clusters(member_pubkeys, edges, min_size=2, reputation=None):
    """GrapeRank-weighted label propagation community detection.

    Edges are weighted by the source member's reputation score so that
    follows from highly-trusted members pull harder during propagation.

    Args:
        member_pubkeys: list of hex pubkeys (directory members only)
        edges: list of (source, target) tuples (follow edges)
        min_size: clusters smaller than this become unclustered (-1)
        reputation: dict pubkey -> reputation_score (0-100), or None for unweighted

    Returns:
        dict mapping pubkey -> cluster_id (int >= 0, or -1 for unclustered)
    """
    member_set = set(member_pubkeys)
    if len(member_set) < 3:
        return {pk: -1 for pk in member_set}

    if reputation is None:
        reputation = {}

    # Normalize reputation to 0.1-1.0 range (floor of 0.1 so even unscored members have some pull)
    def _weight(pk):
        return max(0.1, (reputation.get(pk, 0) / 100.0))

    # Build undirected weighted adjacency list (only edges between directory members)
    # Each neighbor entry is (pubkey, weight) where weight = source's reputation
    adj = {pk: [] for pk in member_set}
    for src, tgt in edges:
        if src in member_set and tgt in member_set and src != tgt:
            adj[src].append((tgt, _weight(src)))
            adj[tgt].append((src, _weight(tgt)))

    # Initialize: each node is its own label
    labels = {pk: i for i, pk in enumerate(sorted(member_set))}

    # Iterate weighted label propagation
    sorted_pks = sorted(member_set)
    for _ in range(20):
        changed = False
        for pk in sorted_pks:
            neighbors = adj[pk]
            if not neighbors:
                continue
            # Sum weights per neighbor label (not just count)
            weight_sums = {}
            for npk, w in neighbors:
                lbl = labels[npk]
                weight_sums[lbl] = weight_sums.get(lbl, 0.0) + w
            # Highest weight wins, ties broken by smallest label
            best_label = min(weight_sums, key=lambda l: (-weight_sums[l], l))
            if labels[pk] != best_label:
                labels[pk] = best_label
                changed = True
        if not changed:
            break

    # Renumber clusters sequentially
    label_to_id = {}
    next_id = 0
    for pk in sorted_pks:
        lbl = labels[pk]
        if lbl not in label_to_id:
            label_to_id[lbl] = next_id
            next_id += 1
        labels[pk] = label_to_id[lbl]

    # Count cluster sizes
    sizes = Counter(labels.values())

    # If only 1 cluster, clustering is meaningless
    if len(sizes) <= 1:
        return {pk: -1 for pk in member_set}

    # Small clusters and isolated nodes become unclustered
    for pk in sorted_pks:
        cid = labels[pk]
        if sizes[cid] < min_size or not adj[pk]:
            labels[pk] = -1

    # Renumber again (skip -1)
    used = sorted(set(v for v in labels.values() if v >= 0))
    remap = {old: new for new, old in enumerate(used)}
    remap[-1] = -1
    return {pk: remap[labels[pk]] for pk in member_set}


def generate_cluster_labels(assignments, members):
    """Auto-generate labels and colors for each cluster.

    Args:
        assignments: dict pubkey -> cluster_id
        members: list of dicts with pubkey, about, tags (list), trust_count, name

    Returns:
        dict cluster_id -> {"label": str, "color": str, "member_count": int}
    """
    member_map = {m["pubkey"]: m for m in members}

    # Group members by cluster
    clusters = {}
    for pk, cid in assignments.items():
        if cid < 0:
            continue
        clusters.setdefault(cid, []).append(pk)

    result = {}
    used_labels = set()

    for cid in sorted(clusters):
        pks = clusters[cid]

        # Collect tags
        all_tags = []
        for pk in pks:
            m = member_map.get(pk, {})
            tags = m.get("tags") or []
            if isinstance(tags, str):
                import json
                try:
                    tags = json.loads(tags)
                except Exception:
                    tags = []
            all_tags.extend(t.strip().lower() for t in tags if t.strip())

        tag_counts = Counter(all_tags)
        label = None

        # Try dominant tag (>40% of cluster members)
        if tag_counts:
            top_tag, top_count = tag_counts.most_common(1)[0]
            if top_count / len(pks) > 0.4 and top_tag not in used_labels:
                label = top_tag.title()
                used_labels.add(top_tag)

        # Try bio keywords
        if not label:
            word_counts = Counter()
            for pk in pks:
                m = member_map.get(pk, {})
                about = (m.get("about") or "").lower()
                words = _WORD_RE.findall(about)
                word_counts.update(w for w in words if w not in _STOPWORDS)

            for word, _ in word_counts.most_common(10):
                if word not in used_labels:
                    label = word.title()
                    used_labels.add(word)
                    break

        # Fallback: most-followed member's name
        if not label:
            best_pk = max(pks, key=lambda pk: member_map.get(pk, {}).get("trust_count", 0))
            best_name = member_map.get(best_pk, {}).get("name", "")
            if best_name:
                label = f"{best_name}'s Circle"
            else:
                label = f"Group {chr(65 + cid % 26)}"

        result[cid] = {
            "label": label,
            "color": CLUSTER_COLORS[cid % len(CLUSTER_COLORS)],
            "member_count": len(pks),
        }

    return result
