#!/usr/bin/env python3
"""
Dataset Generator for Scientific Research Papers Graph Database

Generates synthetic data matching the following schema:
- Nodes: PublishDate, Publication, Journal, Concept, Section, Author
- Relationships: PART_OF, SUPPORTS/CONTRADICTS/DISCUSSES/CITES, CITES_PAPER, SYNONYM_OF, SUBFIELD_OF, PUBLISHED_IN, AUTHORED_BY, PUBLISHED_ON

Uses NetworkX for realistic graph distribution patterns.
Uses Faker for realistic property generation.
Uses multiprocessing for parallel node generation.
Memory efficient: exports each node/relationship type immediately after generation.
"""

import argparse
import multiprocessing as mp
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import networkx as nx
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from faker.providers import BaseProvider

# Vector dimensions for embeddings
VECTOR_DIMENSIONS = 1536


def generate_vector(rng: random.Random | None = None, dimensions: int = VECTOR_DIMENSIONS) -> list[float]:
    """Generate a random normalized vector with the specified dimensions."""
    if rng is None:
        vec = np.random.randn(dimensions).astype(np.float32)
    else:
        vec = np.array([rng.gauss(0, 1) for _ in range(dimensions)], dtype=np.float32)
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec.tolist()


# Default node counts (matching original dataset scale)
DEFAULT_COUNTS = {
    "publish_date": 2040,
    "publication": 25162369,
    "journal": 82230,
    "concept": 815467,
    "section": 26488785,
    "author": 7524688,
}

# Relationship ratios relative to source node count
RELATIONSHIP_RATIOS = {
    "section_part_of_publication": 1.05,
    "section_cites": 0.032,
    "section_contradicts": 0.26,
    "section_supports": 0.69,
    "section_discusses": 1.25,
    "publication_cites_paper": 0.18,
    "concept_synonym_of": 0.02,
    "concept_subfield_of": 0.0002,
    "publication_published_in": 0.91,
    "publication_authored_by": 1.21,
    "publication_published_on": 0.97,
}

# Scientific journals
JOURNALS = [
    "Nature",
    "Science",
    "Cell",
    "PNAS",
    "Physical Review Letters",
    "The Lancet",
    "New England Journal of Medicine",
    "IEEE Transactions",
    "ACM Computing Surveys",
    "arXiv",
]

PUBLICATION_TYPES = [
    "Research Article",
    "Review Article",
    "Conference Paper",
    "Preprint",
    "Case Study",
    "Meta-Analysis",
    "Letter",
    "Technical Report",
]

CONCEPT_TYPES = ["METHOD", "THEORY", "PHENOMENON", "METRIC", "ALGORITHM", "DISEASE"]
CONCEPT_SOURCES = ["pubmed", "arxiv", "semantic_scholar", "crossref", "manual"]

RESEARCH_FIELDS = [
    "machine learning",
    "deep learning",
    "neural networks",
    "computer vision",
    "natural language processing",
    "reinforcement learning",
    "optimization",
    "quantum computing",
    "bioinformatics",
    "genomics",
    "proteomics",
    "epidemiology",
    "immunology",
    "oncology",
    "cardiology",
    "neuroscience",
    "materials science",
    "nanotechnology",
    "renewable energy",
    "climate science",
]

RESEARCH_METHODS = [
    "regression analysis",
    "classification",
    "clustering",
    "dimensionality reduction",
    "cross-validation",
    "bootstrapping",
    "Bayesian inference",
    "Monte Carlo simulation",
    "A/B testing",
    "meta-analysis",
    "systematic review",
    "randomized controlled trial",
    "cohort study",
    "case-control study",
    "longitudinal study",
]

RESEARCH_METRICS = [
    "accuracy",
    "precision",
    "recall",
    "F1-score",
    "AUC-ROC",
    "p-value",
    "confidence interval",
    "effect size",
    "statistical significance",
    "correlation coefficient",
    "R-squared",
    "mean squared error",
]

RESEARCH_VERBS = [
    "investigated",
    "analyzed",
    "evaluated",
    "compared",
    "demonstrated",
    "proposed",
    "developed",
    "implemented",
    "validated",
    "replicated",
    "extended",
    "optimized",
    "synthesized",
    "characterized",
    "modeled",
]

RESEARCH_FINDINGS = [
    "significant improvement",
    "novel approach",
    "state-of-the-art results",
    "promising outcomes",
    "preliminary evidence",
    "inconclusive results",
    "negative correlation",
    "positive association",
    "causal relationship",
    "unexpected findings",
    "robust performance",
    "limited generalizability",
]


class ScientificContentProvider(BaseProvider):
    """Custom Faker provider for scientific research content."""

    def researcher_name(self) -> str:
        first = self.generator.first_name()
        last = self.generator.last_name()
        if self.random_int(0, 10) > 6:
            middle = f" {self.random_uppercase_letter()}."
            return f"{first}{middle} {last}"
        return f"{first} {last}"

    def orcid_id(self) -> str:
        parts = [self.generator.numerify("####") for _ in range(4)]
        return "-".join(parts)

    def doi(self) -> str:
        prefix = self.random_element(["10.1038", "10.1126", "10.1016", "10.1109", "10.1145", "10.1371", "10.1007"])
        suffix = self.generator.lexify("?????").lower() + "." + self.generator.numerify("####")
        return f"{prefix}/{suffix}"

    def paper_title(self) -> str:
        templates = [
            lambda: f"A {self.random_element(['Novel', 'Comprehensive', 'Systematic', 'Comparative', 'Unified'])} Approach to {self.random_element(RESEARCH_FIELDS).title()}",
            lambda: f"{self.random_element(RESEARCH_FIELDS).title()}: {self.random_element(['Methods', 'Applications', 'Challenges', 'Advances', 'Perspectives'])} and {self.random_element(['Future Directions', 'Open Problems', 'Opportunities', 'Limitations'])}",
            lambda: f"Investigating the {self.random_element(['Role', 'Impact', 'Effect', 'Influence'])} of {self.random_element(RESEARCH_FIELDS).title()} on {self.random_element(RESEARCH_FIELDS).title()}",
            lambda: f"{self.random_element(['Deep', 'Machine', 'Statistical', 'Computational'])} Learning for {self.random_element(RESEARCH_FIELDS).title()}",
            lambda: f"Understanding {self.random_element(RESEARCH_FIELDS).title()} through {self.random_element(RESEARCH_METHODS).title()}",
            lambda: f"A {self.random_element(RESEARCH_METHODS).title()} Study of {self.random_element(RESEARCH_FIELDS).title()}",
        ]
        return self.random_element(templates)()

    def abstract_text(self, min_sentences: int = 4, max_sentences: int = 8) -> str:
        num_sentences = self.random_int(min_sentences, max_sentences)
        sentences = [
            f"{self.random_element(RESEARCH_FIELDS).title()} has emerged as a critical area of research in recent years."
        ]
        templates = [
            lambda: f"We {self.random_element(RESEARCH_VERBS)} a {self.random_element(['novel', 'new', 'improved', 'modified'])} {self.random_element(RESEARCH_METHODS)} approach.",
            lambda: f"Our method achieves {self.random_element(RESEARCH_FINDINGS)} compared to existing baselines.",
            lambda: f"The proposed framework demonstrates {self.random_element(['superior', 'comparable', 'improved', 'state-of-the-art'])} {self.random_element(RESEARCH_METRICS)} of {self.random_int(85, 99)}.{self.random_int(0, 9)}%.",
            lambda: f"We conducted experiments on {self.random_int(3, 10)} benchmark datasets spanning {self.random_element(RESEARCH_FIELDS)}.",
            lambda: f"Statistical analysis revealed a {self.random_element(['significant', 'strong', 'moderate', 'weak'])} {self.random_element(['correlation', 'association', 'relationship'])} (p < 0.{self.random_element(['001', '01', '05'])}).",
            lambda: f"Results indicate {self.random_element(RESEARCH_FINDINGS)} in the context of {self.random_element(RESEARCH_FIELDS)}.",
            lambda: f"Our findings suggest that {self.random_element(RESEARCH_METHODS)} can effectively address challenges in {self.random_element(RESEARCH_FIELDS)}.",
            lambda: self.generator.sentence(),
        ]
        for _ in range(num_sentences - 1):
            sentences.append(self.random_element(templates)())
        return " ".join(sentences)

    def section_text(self, section_type: str = "methods") -> str:
        if section_type == "introduction":
            return " ".join(
                [
                    f"{self.random_element(RESEARCH_FIELDS).title()} represents a fundamental challenge in modern science.",
                    f"Previous work has {self.random_element(RESEARCH_VERBS)} various approaches to this problem.",
                    f"However, existing methods suffer from {self.random_element(['limited scalability', 'poor generalization', 'high computational cost', 'data requirements'])}.",
                    f"In this paper, we propose a {self.random_element(['novel', 'new', 'unified', 'comprehensive'])} approach based on {self.random_element(RESEARCH_METHODS)}.",
                ]
            )
        elif section_type == "methods":
            return " ".join(
                [
                    f"We formulate the problem as a {self.random_element(['optimization', 'classification', 'regression', 'prediction'])} task.",
                    f"Our model consists of {self.random_int(2, 5)} components.",
                    f"We employ {self.random_element(RESEARCH_METHODS)} for parameter estimation.",
                    f"The {self.random_element(RESEARCH_METRICS)} is computed as the primary evaluation metric.",
                ]
            )
        elif section_type == "results":
            return " ".join(
                [
                    f"Our method achieves {self.random_element(RESEARCH_METRICS)} of {self.random_int(85, 98)}.{self.random_int(0, 9)}% on the test set.",
                    f"This represents a {self.random_int(2, 15)}% improvement over the previous state-of-the-art.",
                    f"Statistical significance was confirmed (p < 0.{self.random_element(['001', '01', '05'])}).",
                ]
            )
        elif section_type == "discussion":
            return " ".join(
                [
                    f"Our results demonstrate {self.random_element(RESEARCH_FINDINGS)} in {self.random_element(RESEARCH_FIELDS)}.",
                    f"Limitations include {self.random_element(['limited dataset size', 'domain-specific assumptions', 'computational requirements'])}.",
                    f"Future work should explore {self.random_element(['transfer learning', 'multi-task learning', 'larger-scale experiments'])}.",
                ]
            )
        return self.abstract_text(3, 6)

    def concept_term(self, concept_type: str = "METHOD") -> str:
        if concept_type == "METHOD":
            return f"{self.random_element(['Adaptive', 'Recursive', 'Iterative', 'Probabilistic', 'Stochastic'])} {self.random_element(RESEARCH_METHODS).title()}"
        elif concept_type == "THEORY":
            return f"{self.random_element(['Unified', 'General', 'Quantum', 'Statistical'])} {self.random_element(['Field Theory', 'Framework', 'Model', 'Hypothesis'])}"
        elif concept_type == "PHENOMENON":
            return (
                f"{self.random_element(['Emergent', 'Observed', 'Predicted'])} {self.generator.word().title()} Effect"
            )
        elif concept_type == "METRIC":
            return self.random_element(RESEARCH_METRICS).title()
        elif concept_type == "ALGORITHM":
            return f"{self.random_element(['Fast', 'Parallel', 'Distributed', 'Online'])} {self.generator.word().title()} Algorithm"
        elif concept_type == "DISEASE":
            return f"{self.random_element(['Type', 'Stage', 'Grade'])} {self.random_int(1, 4)} {self.generator.word().title()} Syndrome"
        return self.generator.word().title()

    def concept_definition(self, concept_type: str = "METHOD") -> str:
        templates = {
            "METHOD": f"A computational technique for {self.random_element(RESEARCH_FIELDS)} that employs {self.random_element(RESEARCH_METHODS)}.",
            "THEORY": f"A theoretical framework explaining {self.random_element(['observed phenomena', 'empirical data', 'experimental results'])} in {self.random_element(RESEARCH_FIELDS)}.",
            "PHENOMENON": f"An observed effect characterized by {self.random_element(RESEARCH_FINDINGS)} under specific conditions.",
            "METRIC": f"A quantitative measure used to evaluate {self.random_element(['model performance', 'experimental outcomes', 'statistical significance'])}.",
            "ALGORITHM": f"A step-by-step procedure for solving {self.random_element(RESEARCH_FIELDS)} problems with {self.random_element(['polynomial', 'logarithmic', 'linear'])} complexity.",
            "DISEASE": f"A medical condition affecting {self.random_element(['cardiovascular', 'neurological', 'respiratory', 'immune'])} systems.",
        }
        return templates.get(concept_type, self.generator.sentence())

    def journal_name(self) -> str:
        prefixes = [
            "Journal of",
            "International Journal of",
            "European Journal of",
            "American Journal of",
            "British Journal of",
            "Asian Journal of",
            "Frontiers in",
            "Advances in",
            "Progress in",
            "Trends in",
            "Current Opinion in",
            "Annual Review of",
            "Quarterly Review of",
        ]
        suffixes = [
            "Letters",
            "Reviews",
            "Research",
            "Studies",
            "Science",
            "Engineering",
            "Technology",
            "Methods",
            "Applications",
            "Proceedings",
            "Transactions",
            "Communications",
            "Reports",
        ]
        orgs = ["ACM", "IEEE", "AAAI", "NeurIPS", "ICML", "CVPR", "ICLR", "EMNLP", "ACL", "NAACL", "SIGKDD", "WWW"]
        patterns = [
            lambda: f"{self.random_element(prefixes)} {self.random_element(RESEARCH_FIELDS).title()}",
            lambda: f"{self.random_element(RESEARCH_FIELDS).title()} {self.random_element(suffixes)}",
            lambda: f"{self.random_element(prefixes)} {self.random_element(RESEARCH_FIELDS).title()} and {self.random_element(RESEARCH_FIELDS).title()}",
            lambda: f"Proceedings of the {self.random_element(orgs)} Conference on {self.random_element(RESEARCH_FIELDS).title()}",
            lambda: f"{self.random_element(orgs)} {self.random_element(suffixes)} on {self.random_element(RESEARCH_FIELDS).title()}",
            lambda: f"{self.generator.word().title()} {self.random_element(RESEARCH_FIELDS).title()} {self.random_element(suffixes)}",
            lambda: f"The {self.generator.word().title()} Journal of {self.random_element(RESEARCH_FIELDS).title()}",
            lambda: f"{self.random_element(RESEARCH_FIELDS).title()}: A {self.random_element(['Multidisciplinary', 'Comprehensive', 'Peer-Reviewed', 'Open Access'])} Journal",
            lambda: f"{self.random_element(prefixes)} {self.generator.word().title()} {self.random_element(RESEARCH_FIELDS).title()}",
            lambda: f"{self.generator.company()} {self.random_element(suffixes)} in {self.random_element(RESEARCH_FIELDS).title()}",
        ]
        return self.random_element(patterns)()


# =============================================================================
# Worker functions for multiprocessing
# =============================================================================


def _init_worker(seed: int, worker_id: int) -> tuple[Faker, random.Random]:
    worker_seed = seed + worker_id
    fake = Faker()
    Faker.seed(worker_seed)
    fake.add_provider(ScientificContentProvider)
    rng = random.Random(worker_seed)
    return fake, rng


def _generate_authors_batch(args: tuple) -> list[dict]:
    start_idx, batch_size, seed, worker_id = args
    fake, rng = _init_worker(seed, worker_id)
    authors = []
    for _ in range(batch_size):
        author_id = str(uuid.uuid4())
        orcid = fake.orcid_id()
        citation_count = int(rng.paretovariate(1.5) - 1) * 10
        citation_count = max(0, min(citation_count, 100000))
        h_index = max(0, min(100, int(citation_count / 500 + rng.gauss(0, 3))))
        publication_count = max(1, int(h_index * rng.uniform(1.5, 4.0)))
        authors.append(
            {
                "author_id": author_id,
                "orcid": orcid,
                "author_name": fake.researcher_name(),
                "h_index": h_index,
                "citation_count": citation_count,
                "publication_count": publication_count,
                "orcid_url": f"https://orcid.org/{orcid}",
            }
        )
    return authors


def _generate_concepts_batch(args: tuple) -> list[dict]:
    start_idx, batch_size, seed, worker_id, num_clusters, publish_date_ids, include_vectors = args
    fake, rng = _init_worker(seed, worker_id)
    concepts = []
    for _ in range(batch_size):
        concept_id = str(uuid.uuid4())
        concept_type = rng.choice(CONCEPT_TYPES)
        term = fake.concept_term(concept_type)
        if concept_type == "METHOD":
            synonym = term.split()[-1] if rng.random() > 0.3 else ""
        elif concept_type == "ALGORITHM":
            parts = term.split()
            synonym = parts[-1] if len(parts) > 1 and rng.random() > 0.5 else ""
        elif concept_type == "METRIC":
            synonym = "".join([w[0] for w in term.split()[:3]]).upper() if rng.random() > 0.6 else ""
        else:
            synonym = fake.word().capitalize() if rng.random() > 0.7 else ""
        cluster_id = int(rng.paretovariate(1.2)) % num_clusters
        concept = {
            "concept_id": concept_id,
            "term": term,
            "synonym": synonym,
            "field": concept_type,
            "source": rng.choice(CONCEPT_SOURCES),
            "definition": fake.concept_definition(concept_type) if rng.random() > 0.3 else "",
            "abstract_concept": rng.random() < 0.1,
            "cluster_id": cluster_id,
            "first_mentioned_date": rng.choice(publish_date_ids) if publish_date_ids else 20200101,
        }
        if include_vectors:
            concept["term_vector"] = generate_vector(rng)
            concept["synonym_vector"] = generate_vector(rng)
            concept["combined_vector"] = generate_vector(rng)
        concepts.append(concept)
    return concepts


def _make_publication_doi(idx: int) -> str:
    """Generate deterministic DOI based on index."""
    return f"10.1000/pub.{idx:010d}"


def _make_section_id(idx: int) -> str:
    """Generate deterministic section ID based on index."""
    return f"sec-{idx:010d}"


def _generate_publications_batch(args: tuple) -> list[dict]:
    start_idx, batch_size, seed, worker_id, journal_ids, base_timestamp = args
    fake, rng = _init_worker(seed, worker_id)
    publications = []
    for i in range(batch_size):
        idx = start_idx + i
        doi = _make_publication_doi(idx)
        journal = rng.choice(journal_ids) if journal_ids else "arXiv"
        paper_type = rng.choice(PUBLICATION_TYPES)
        time_offset = idx * 3600 + rng.randint(0, 3600)
        timestamp = base_timestamp + time_offset
        citation_count = int(rng.paretovariate(1.5) - 1) * 5
        citation_count = max(0, min(citation_count, 10000))
        reference_count = rng.randint(10, 80)
        if paper_type == "Preprint":
            paper_url = f"https://arxiv.org/abs/{rng.randint(1000, 9999)}.{rng.randint(10000, 99999)}"
        elif "Conference" in paper_type:
            conf = rng.choice(["neurips", "icml", "cvpr", "acl", "emnlp"])
            paper_url = f"https://proceedings.{conf}.cc/paper/{rng.randint(2015, 2024)}/{idx:08d}"
        else:
            paper_url = f"https://doi.org/{doi}"
        publications.append(
            {
                "doi": doi,
                "paper_url": paper_url,
                "publication_timestamp": timestamp,
                "paper_type": paper_type,
                "journal": journal,
                "citation_count": citation_count,
                "reference_count": reference_count,
                "last_cited_date": timestamp + rng.randint(0, 31536000) if citation_count > 0 else 0,
                "is_preprint": "yes" if paper_type == "Preprint" else "no",
                "title": fake.paper_title(),
                "abstract": fake.abstract_text(),
            }
        )
    return publications


def _generate_sections_batch(args: tuple) -> list[dict]:
    start_idx, batch_size, seed, worker_id = args
    fake, rng = _init_worker(seed, worker_id)
    section_types = ["introduction", "methods", "results", "discussion", "conclusion"]
    sections = []
    for i in range(batch_size):
        idx = start_idx + i
        section_id = _make_section_id(idx)
        section_type = rng.choice(section_types)
        title = fake.paper_title()
        text = fake.section_text(section_type)
        sections.append(
            {
                "section_id": section_id,
                "section_number": rng.randint(1, 10),
                "section_type": section_type,
                "section_text": f"Title: {title}\nSection: {section_type.title()}\nText: {text}",
            }
        )
    return sections


def _generate_journals_batch(args: tuple) -> list[dict]:
    start_idx, batch_size, seed, worker_id = args
    fake, rng = _init_worker(seed, worker_id)
    journals = []
    for i in range(batch_size):
        idx = start_idx + i
        base_name = fake.journal_name()
        journal_name = f"{base_name} (Vol. {idx})"
        journals.append({"journal_name": journal_name})
    return journals


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class GeneratorConfig:
    output_dir: str = "./generated_data"
    scale: float = 1.0
    seed: int = 42
    include_vectors: bool = False
    workers: int = 8

    publish_date_count: int = DEFAULT_COUNTS["publish_date"]
    publication_count: int = DEFAULT_COUNTS["publication"]
    journal_count: int = DEFAULT_COUNTS["journal"]
    concept_count: int = DEFAULT_COUNTS["concept"]
    section_count: int = DEFAULT_COUNTS["section"]
    author_count: int = DEFAULT_COUNTS["author"]

    def get_scaled_count(self, base_count: int) -> int:
        return max(1, int(base_count * self.scale))


# =============================================================================
# Main Generator Class (Memory Efficient)
# =============================================================================


class DatasetGenerator:
    """Generates synthetic graph dataset. Memory efficient: exports immediately after each type."""

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.output_dir = Path(config.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        random.seed(config.seed)

        self.fake = Faker()
        Faker.seed(config.seed)
        self.fake.add_provider(ScientificContentProvider)

        # Only store IDs (not full data) for relationship generation
        self.publish_date_ids: list[int] = []
        self.journal_ids: list[str] = []
        self.author_ids: list[str] = []
        self.concept_ids: list[str] = []

        # For large node types, only store count (IDs are deterministic)
        self.publication_count: int = 0
        self.section_count: int = 0

        # Counters for summary
        self.counts = {}

    def _create_batches(self, total: int, num_workers: int, max_batch_size: int = 50000) -> list[tuple[int, int]]:
        """Create many small batches for memory-efficient processing."""
        batches = []
        start = 0
        while start < total:
            size = min(max_batch_size, total - start)
            batches.append((start, size))
            start += size
        return batches

    def _write_parquet(self, filename: str, data: list[dict], fieldnames: list[str]) -> None:
        filepath = str(self.output_dir / filename)
        if not data:
            arrays = {field: pa.array([], type=pa.string()) for field in fieldnames}
            table = pa.table(arrays)
        else:
            columns = {field: [] for field in fieldnames}
            for row in data:
                for field in fieldnames:
                    columns[field].append(row.get(field))
            table = pa.table(columns)
        pq.write_table(table, filepath, compression="snappy")
        print(f"  Exported {filename}")

    def _create_parquet_writer(self, filename: str, schema: pa.Schema) -> pq.ParquetWriter:
        """Create a ParquetWriter for streaming writes."""
        filepath = str(self.output_dir / filename)
        return pq.ParquetWriter(filepath, schema, compression="snappy")

    def _write_batch_to_parquet(self, writer: pq.ParquetWriter, data: list[dict], fieldnames: list[str]) -> None:
        """Write a batch of data to an open ParquetWriter."""
        if not data:
            return
        columns = {field: [] for field in fieldnames}
        for row in data:
            for field in fieldnames:
                columns[field].append(row.get(field))
        table = pa.table(columns)
        writer.write_table(table)

    def generate_and_export_publish_dates(self) -> None:
        count = self.config.get_scaled_count(self.config.publish_date_count)
        base_date = datetime(2010, 1, 1)
        data = []
        for i in range(count):
            date_obj = base_date + timedelta(days=i)
            date_int = int(date_obj.strftime("%Y%m%d"))
            data.append({"publish_date": date_int})
            self.publish_date_ids.append(date_int)
        self._write_parquet("publish_dates.parquet", data, ["publish_date"])
        self.counts["PublishDate"] = len(data)
        print(f"Generated {len(data):,} PublishDate nodes")

    def generate_and_export_journals(self) -> None:
        count = self.config.get_scaled_count(self.config.journal_count)
        fieldnames = ["journal_name"]
        schema = pa.schema([("journal_name", pa.string())])

        writer = self._create_parquet_writer("journals.parquet", schema)
        total_count = 0

        # Add seed journals first
        seed_batch = []
        for journal in JOURNALS:
            if total_count >= count:
                break
            seed_batch.append({"journal_name": journal})
            self.journal_ids.append(journal)
            total_count += 1

        if seed_batch:
            self._write_batch_to_parquet(writer, seed_batch, fieldnames)

        remaining = count - total_count
        if remaining > 0:
            batches = self._create_batches(remaining, self.config.workers)
            args = [(start + len(JOURNALS), size, self.config.seed, i) for i, (start, size) in enumerate(batches)]
            print(f"Generating {count:,} Journal nodes with {self.config.workers} workers...")

            with mp.Pool(self.config.workers) as pool:
                for batch in pool.imap_unordered(_generate_journals_batch, args):
                    for journal in batch:
                        self.journal_ids.append(journal["journal_name"])
                    self._write_batch_to_parquet(writer, batch, fieldnames)
                    total_count += len(batch)
                    del batch

        writer.close()
        self.counts["Journal"] = total_count
        print(f"  Exported journals.parquet")
        print(f"Generated {total_count:,} Journal nodes")

    def generate_and_export_authors(self) -> None:
        count = self.config.get_scaled_count(self.config.author_count)
        batches = self._create_batches(count, self.config.workers)
        args = [(start, size, self.config.seed, i) for i, (start, size) in enumerate(batches)]

        fieldnames = [
            "author_id",
            "orcid",
            "author_name",
            "h_index",
            "citation_count",
            "publication_count",
            "orcid_url",
        ]
        schema = pa.schema(
            [
                ("author_id", pa.string()),
                ("orcid", pa.string()),
                ("author_name", pa.string()),
                ("h_index", pa.int64()),
                ("citation_count", pa.int64()),
                ("publication_count", pa.int64()),
                ("orcid_url", pa.string()),
            ]
        )

        print(f"Generating {count:,} Author nodes with {self.config.workers} workers...")
        writer = self._create_parquet_writer("authors.parquet", schema)
        total_count = 0

        with mp.Pool(self.config.workers) as pool:
            for batch in pool.imap_unordered(_generate_authors_batch, args):
                for author in batch:
                    self.author_ids.append(author["author_id"])
                self._write_batch_to_parquet(writer, batch, fieldnames)
                total_count += len(batch)
                del batch  # Explicit cleanup

        writer.close()
        self.counts["Author"] = total_count
        print(f"  Exported authors.parquet")
        print(f"Generated {total_count:,} Author nodes")

    def generate_and_export_concepts(self) -> None:
        count = self.config.get_scaled_count(self.config.concept_count)
        num_clusters = max(10, count // 100)
        batches = self._create_batches(count, self.config.workers)
        args = [
            (start, size, self.config.seed, i, num_clusters, self.publish_date_ids, self.config.include_vectors)
            for i, (start, size) in enumerate(batches)
        ]

        fieldnames = [
            "concept_id",
            "term",
            "synonym",
            "field",
            "source",
            "definition",
            "abstract_concept",
            "cluster_id",
            "first_mentioned_date",
        ]
        schema_fields = [
            ("concept_id", pa.string()),
            ("term", pa.string()),
            ("synonym", pa.string()),
            ("field", pa.string()),
            ("source", pa.string()),
            ("definition", pa.string()),
            ("abstract_concept", pa.bool_()),
            ("cluster_id", pa.int64()),
            ("first_mentioned_date", pa.int64()),
        ]
        if self.config.include_vectors:
            fieldnames.extend(["term_vector", "synonym_vector", "combined_vector"])
            schema_fields.extend(
                [
                    ("term_vector", pa.list_(pa.float32())),
                    ("synonym_vector", pa.list_(pa.float32())),
                    ("combined_vector", pa.list_(pa.float32())),
                ]
            )
        schema = pa.schema(schema_fields)

        print(f"Generating {count:,} Concept nodes with {self.config.workers} workers...")
        writer = self._create_parquet_writer("concepts.parquet", schema)
        total_count = 0

        with mp.Pool(self.config.workers) as pool:
            for batch in pool.imap_unordered(_generate_concepts_batch, args):
                for concept in batch:
                    self.concept_ids.append(concept["concept_id"])
                self._write_batch_to_parquet(writer, batch, fieldnames)
                total_count += len(batch)
                del batch

        writer.close()
        self.counts["Concept"] = total_count
        print(f"  Exported concepts.parquet")
        print(f"Generated {total_count:,} Concept nodes")

    def generate_and_export_publications(self) -> None:
        count = self.config.get_scaled_count(self.config.publication_count)
        base_timestamp = 1262304000
        batches = self._create_batches(count, self.config.workers)
        args = [
            (start, size, self.config.seed, i, self.journal_ids, base_timestamp)
            for i, (start, size) in enumerate(batches)
        ]

        fieldnames = [
            "doi",
            "paper_url",
            "publication_timestamp",
            "paper_type",
            "journal",
            "citation_count",
            "reference_count",
            "last_cited_date",
            "is_preprint",
            "title",
            "abstract",
        ]
        schema = pa.schema(
            [
                ("doi", pa.string()),
                ("paper_url", pa.string()),
                ("publication_timestamp", pa.int64()),
                ("paper_type", pa.string()),
                ("journal", pa.string()),
                ("citation_count", pa.int64()),
                ("reference_count", pa.int64()),
                ("last_cited_date", pa.int64()),
                ("is_preprint", pa.string()),
                ("title", pa.string()),
                ("abstract", pa.string()),
            ]
        )

        print(f"Generating {count:,} Publication nodes with {self.config.workers} workers...")
        writer = self._create_parquet_writer("publications.parquet", schema)
        total_count = 0

        with mp.Pool(self.config.workers) as pool:
            for batch in pool.imap_unordered(_generate_publications_batch, args):
                self._write_batch_to_parquet(writer, batch, fieldnames)
                total_count += len(batch)
                del batch

        writer.close()
        # Store count for deterministic ID regeneration, not the actual IDs
        self.publication_count = count
        self.counts["Publication"] = total_count
        print(f"  Exported publications.parquet")
        print(f"Generated {total_count:,} Publication nodes")

    def generate_and_export_sections(self) -> None:
        count = self.config.get_scaled_count(self.config.section_count)
        batches = self._create_batches(count, self.config.workers)
        args = [(start, size, self.config.seed, i) for i, (start, size) in enumerate(batches)]

        fieldnames = ["section_id", "section_number", "section_type", "section_text"]
        schema = pa.schema(
            [
                ("section_id", pa.string()),
                ("section_number", pa.int64()),
                ("section_type", pa.string()),
                ("section_text", pa.string()),
            ]
        )

        print(f"Generating {count:,} Section nodes with {self.config.workers} workers...")
        writer = self._create_parquet_writer("sections.parquet", schema)
        total_count = 0

        with mp.Pool(self.config.workers) as pool:
            for batch in pool.imap_unordered(_generate_sections_batch, args):
                self._write_batch_to_parquet(writer, batch, fieldnames)
                total_count += len(batch)
                del batch

        writer.close()
        # Store count for deterministic ID regeneration, not the actual IDs
        self.section_count = count
        self.counts["Section"] = total_count
        print(f"  Exported sections.parquet")
        print(f"Generated {total_count:,} Section nodes")

    def generate_and_export_section_part_of(self) -> None:
        print("Generating PART_OF relationships...")
        BATCH_SIZE = 100000
        PUB_BATCH_SIZE = 100000
        fieldnames = ["section_id", "publication_doi"]
        schema = pa.schema([(f, pa.string()) for f in fieldnames])

        rng = np.random.default_rng(self.config.seed)

        writer = self._create_parquet_writer("rel_part_of.parquet", schema)
        batch = []
        total_count = 0
        section_idx = 0

        # Process publications in batches
        for pub_batch_start in range(0, self.publication_count, PUB_BATCH_SIZE):
            if section_idx >= self.section_count:
                break

            pub_batch_end = min(pub_batch_start + PUB_BATCH_SIZE, self.publication_count)
            pub_batch_count = pub_batch_end - pub_batch_start

            # Pre-generate section counts for this batch using numpy
            # Pareto(2.0) + 1 gives minimum 1, then clip to max 20
            num_sections_batch = np.clip((rng.pareto(2.0, pub_batch_count) + 1).astype(int), 1, 20)

            for i, pub_idx in enumerate(range(pub_batch_start, pub_batch_end)):
                if section_idx >= self.section_count:
                    break

                pub_id = _make_publication_doi(pub_idx)
                num_sections = num_sections_batch[i]

                for _ in range(num_sections):
                    if section_idx >= self.section_count:
                        break
                    section_id = _make_section_id(section_idx)
                    batch.append({"section_id": section_id, "publication_doi": pub_id})
                    section_idx += 1
                    if len(batch) >= BATCH_SIZE:
                        self._write_batch_to_parquet(writer, batch, fieldnames)
                        total_count += len(batch)
                        batch = []

        # Remaining sections go to random publications
        remaining = self.section_count - section_idx
        if remaining > 0:
            random_pub_indices = rng.integers(0, self.publication_count, size=remaining)
            for i in range(remaining):
                pub_id = _make_publication_doi(random_pub_indices[i])
                section_id = _make_section_id(section_idx)
                batch.append({"section_id": section_id, "publication_doi": pub_id})
                section_idx += 1
                if len(batch) >= BATCH_SIZE:
                    self._write_batch_to_parquet(writer, batch, fieldnames)
                    total_count += len(batch)
                    batch = []

        if batch:
            self._write_batch_to_parquet(writer, batch, fieldnames)
            total_count += len(batch)

        writer.close()
        self.counts["PART_OF"] = total_count
        print(f"  Exported rel_part_of.parquet")
        print(f"Generated {total_count:,} PART_OF relationships")

    def generate_and_export_section_references(self) -> None:
        print("Generating SUPPORTS/CONTRADICTS/DISCUSSES/CITES relationships...")
        BATCH_SIZE = 100000
        fieldnames = ["section_id", "concept_id", "type", "confidence_score"]
        schema = pa.schema(
            [
                ("section_id", pa.string()),
                ("concept_id", pa.string()),
                ("type", pa.string()),
                ("confidence_score", pa.float64()),
            ]
        )

        reference_types = [
            ("CITES", RELATIONSHIP_RATIOS["section_cites"]),
            ("CONTRADICTS", RELATIONSHIP_RATIOS["section_contradicts"]),
            ("SUPPORTS", RELATIONSHIP_RATIOS["section_supports"]),
            ("DISCUSSES", RELATIONSHIP_RATIOS["section_discusses"]),
        ]

        # Use numpy for fast weighted selection
        rng = np.random.default_rng(self.config.seed)
        concept_weights = rng.pareto(1.5, len(self.concept_ids))
        concept_probs = concept_weights / concept_weights.sum()
        concept_ids_arr = np.array(self.concept_ids)

        writer = self._create_parquet_writer("rel_references.parquet", schema)
        batch = []
        total_count = 0

        for reference_type, ratio in reference_types:
            count = int(self.section_count * ratio)
            print(f"  Generating {count:,} {reference_type}...")

            # Pre-generate all random values in batches
            for batch_start in range(0, count, BATCH_SIZE):
                batch_count = min(BATCH_SIZE, count - batch_start)

                # Batch generate random values using numpy
                section_indices = rng.integers(0, self.section_count, size=batch_count)
                concept_indices = rng.choice(len(concept_ids_arr), size=batch_count, p=concept_probs)
                confidence_scores = np.round(rng.uniform(0.1, 1.0, size=batch_count), 3)

                for i in range(batch_count):
                    batch.append(
                        {
                            "section_id": _make_section_id(section_indices[i]),
                            "concept_id": concept_ids_arr[concept_indices[i]],
                            "type": reference_type,
                            "confidence_score": confidence_scores[i],
                        }
                    )

                self._write_batch_to_parquet(writer, batch, fieldnames)
                total_count += len(batch)
                batch = []

        writer.close()
        self.counts["*_REFERENCES"] = total_count
        print(f"  Exported rel_references.parquet")
        print(f"Generated {total_count:,} reference relationships")

    def generate_and_export_publication_cites_paper(self) -> None:
        print("Generating CITES_PAPER relationships...")
        BATCH_SIZE = 100000
        fieldnames = ["citing_doi", "cited_doi"]
        schema = pa.schema([(f, pa.string()) for f in fieldnames])

        count = int(self.publication_count * RELATIONSHIP_RATIOS["publication_cites_paper"])
        if count == 0 or self.publication_count < 2:
            self.counts["CITES_PAPER"] = 0
            return

        writer = self._create_parquet_writer("rel_cites_paper.parquet", schema)
        batch = []
        total_count = 0

        # Generate citation trees using index ranges instead of storing all IDs
        num_trees = max(1, count // 10)
        indices = list(range(self.publication_count))
        random.shuffle(indices)

        idx_pos = 0
        for tree_idx in range(num_trees):
            if idx_pos >= len(indices) or total_count >= count:
                break
            tree_size = min(len(indices) - idx_pos, random.randint(2, 20))
            if tree_size < 2:
                continue
            tree = nx.random_tree(tree_size)
            tree_indices = indices[idx_pos : idx_pos + tree_size]
            idx_pos += tree_size

            for u, v in tree.edges():
                if u > v:
                    u, v = v, u
                citing_doi = _make_publication_doi(tree_indices[v])
                cited_doi = _make_publication_doi(tree_indices[u])
                batch.append({"citing_doi": citing_doi, "cited_doi": cited_doi})
                if len(batch) >= BATCH_SIZE:
                    self._write_batch_to_parquet(writer, batch, fieldnames)
                    total_count += len(batch)
                    batch = []

            # Free memory periodically
            if tree_idx % 1000 == 0:
                del tree_indices

        if batch:
            self._write_batch_to_parquet(writer, batch, fieldnames)
            total_count += len(batch)

        writer.close()
        self.counts["CITES_PAPER"] = total_count
        print(f"  Exported rel_cites_paper.parquet")
        print(f"Generated {total_count:,} CITES_PAPER relationships")

    def generate_and_export_concept_synonym_of(self) -> None:
        print("Generating SYNONYM_OF relationships...")
        BATCH_SIZE = 100000
        fieldnames = ["concept_id", "synonym_id"]
        schema = pa.schema([(f, pa.string()) for f in fieldnames])

        count = int(len(self.concept_ids) * RELATIONSHIP_RATIOS["concept_synonym_of"])
        if count == 0 or len(self.concept_ids) < 2:
            self.counts["SYNONYM_OF"] = 0
            return

        writer = self._create_parquet_writer("rel_synonym_of.parquet", schema)
        batch = []
        total_count = 0
        used_pairs = set()

        for _ in range(count * 2):  # Try more times to account for duplicates
            if total_count + len(batch) >= count:
                break
            concept1 = random.choice(self.concept_ids)
            concept2 = random.choice(self.concept_ids)
            if concept1 != concept2 and (concept1, concept2) not in used_pairs:
                batch.append({"concept_id": concept1, "synonym_id": concept2})
                used_pairs.add((concept1, concept2))
                if len(batch) >= BATCH_SIZE:
                    self._write_batch_to_parquet(writer, batch, fieldnames)
                    total_count += len(batch)
                    batch = []

        if batch:
            self._write_batch_to_parquet(writer, batch, fieldnames)
            total_count += len(batch)

        writer.close()
        self.counts["SYNONYM_OF"] = total_count
        print(f"  Exported rel_synonym_of.parquet")
        print(f"Generated {total_count:,} SYNONYM_OF relationships")

    def generate_and_export_concept_subfield_of(self) -> None:
        print("Generating SUBFIELD_OF relationships...")
        BATCH_SIZE = 100000
        fieldnames = ["child_concept_id", "parent_concept_id"]
        schema = pa.schema([(f, pa.string()) for f in fieldnames])

        count = int(len(self.concept_ids) * RELATIONSHIP_RATIOS["concept_subfield_of"])
        if count == 0 or len(self.concept_ids) < 2:
            self.counts["SUBFIELD_OF"] = 0
            return

        writer = self._create_parquet_writer("rel_subfield_of.parquet", schema)
        batch = []
        total_count = 0

        for i in range(min(count, len(self.concept_ids) - 1)):
            batch.append(
                {
                    "child_concept_id": self.concept_ids[i + 1],
                    "parent_concept_id": self.concept_ids[i],
                }
            )
            if len(batch) >= BATCH_SIZE:
                self._write_batch_to_parquet(writer, batch, fieldnames)
                total_count += len(batch)
                batch = []

        if batch:
            self._write_batch_to_parquet(writer, batch, fieldnames)
            total_count += len(batch)

        writer.close()
        self.counts["SUBFIELD_OF"] = total_count
        print(f"  Exported rel_subfield_of.parquet")
        print(f"Generated {total_count:,} SUBFIELD_OF relationships")

    def generate_and_export_publication_published_in(self) -> None:
        print("Generating PUBLISHED_IN relationships...")
        BATCH_SIZE = 100000
        fieldnames = ["publication_doi", "journal_name"]
        schema = pa.schema([(f, pa.string()) for f in fieldnames])

        # Use numpy for fast weighted selection
        rng = np.random.default_rng(self.config.seed)
        journal_weights = rng.pareto(1.2, len(self.journal_ids))
        journal_probs = journal_weights / journal_weights.sum()
        journal_ids_arr = np.array(self.journal_ids)

        # Pre-determine which publications get relationships
        ratio = RELATIONSHIP_RATIOS["publication_published_in"]
        include_mask = rng.random(self.publication_count) < ratio
        pub_indices = np.where(include_mask)[0]

        # Pre-generate all journal selections
        journal_selections = rng.choice(len(journal_ids_arr), size=len(pub_indices), p=journal_probs)

        writer = self._create_parquet_writer("rel_published_in.parquet", schema)
        batch = []
        total_count = 0

        for i, pub_idx in enumerate(pub_indices):
            pub_id = _make_publication_doi(pub_idx)
            batch.append({"publication_doi": pub_id, "journal_name": journal_ids_arr[journal_selections[i]]})
            if len(batch) >= BATCH_SIZE:
                self._write_batch_to_parquet(writer, batch, fieldnames)
                total_count += len(batch)
                batch = []

        if batch:
            self._write_batch_to_parquet(writer, batch, fieldnames)
            total_count += len(batch)

        writer.close()
        self.counts["PUBLISHED_IN"] = total_count
        print(f"  Exported rel_published_in.parquet")
        print(f"Generated {total_count:,} PUBLISHED_IN relationships")

    def generate_and_export_publication_authored_by(self) -> None:
        print("Generating AUTHORED_BY relationships...")
        BATCH_SIZE = 100000
        PUB_BATCH_SIZE = 50000  # Process publications in chunks
        fieldnames = ["publication_doi", "author_id", "author_position", "is_corresponding"]
        schema = pa.schema(
            [
                ("publication_doi", pa.string()),
                ("author_id", pa.string()),
                ("author_position", pa.int64()),
                ("is_corresponding", pa.bool_()),
            ]
        )

        # Use numpy for fast weighted selection
        rng = np.random.default_rng(self.config.seed)
        author_weights = rng.pareto(1.3, len(self.author_ids))
        author_probs = author_weights / author_weights.sum()
        author_ids_arr = np.array(self.author_ids)

        # Author count distribution
        num_author_choices = np.array([1, 2, 3, 4, 5, 6, 7, 8])
        num_author_weights = np.array([15, 25, 25, 15, 10, 5, 3, 2], dtype=float)
        num_author_probs = num_author_weights / num_author_weights.sum()

        writer = self._create_parquet_writer("rel_authored_by.parquet", schema)
        batch = []
        total_count = 0

        # Process in chunks to manage memory
        for pub_batch_start in range(0, self.publication_count, PUB_BATCH_SIZE):
            pub_batch_end = min(pub_batch_start + PUB_BATCH_SIZE, self.publication_count)
            pub_batch_count = pub_batch_end - pub_batch_start

            # Pre-generate author counts for this batch
            num_authors_batch = rng.choice(num_author_choices, size=pub_batch_count, p=num_author_probs)
            total_authors_needed = num_authors_batch.sum()

            # Pre-generate all author selections for this batch
            author_selections = rng.choice(len(author_ids_arr), size=total_authors_needed, p=author_probs)

            author_idx = 0
            for i, pub_idx in enumerate(range(pub_batch_start, pub_batch_end)):
                pub_id = _make_publication_doi(pub_idx)
                num_authors = num_authors_batch[i]

                for pos in range(num_authors):
                    batch.append(
                        {
                            "publication_doi": pub_id,
                            "author_id": author_ids_arr[author_selections[author_idx]],
                            "author_position": pos + 1,
                            "is_corresponding": pos == 0,
                        }
                    )
                    author_idx += 1

                    if len(batch) >= BATCH_SIZE:
                        self._write_batch_to_parquet(writer, batch, fieldnames)
                        total_count += len(batch)
                        batch = []

        if batch:
            self._write_batch_to_parquet(writer, batch, fieldnames)
            total_count += len(batch)

        writer.close()
        self.counts["AUTHORED_BY"] = total_count
        print(f"  Exported rel_authored_by.parquet")
        print(f"Generated {total_count:,} AUTHORED_BY relationships")

    def generate_and_export_publication_published_on(self) -> None:
        print("Generating PUBLISHED_ON relationships...")
        BATCH_SIZE = 100000
        fieldnames = ["publication_doi", "publish_date"]
        schema = pa.schema(
            [
                ("publication_doi", pa.string()),
                ("publish_date", pa.int64()),
            ]
        )

        # Use numpy for fast random selection
        rng = np.random.default_rng(self.config.seed)
        ratio = RELATIONSHIP_RATIOS["publication_published_on"]

        # Pre-determine which publications get relationships
        include_mask = rng.random(self.publication_count) < ratio
        pub_indices = np.where(include_mask)[0]

        # Pre-generate all date selections
        date_ids_arr = np.array(self.publish_date_ids)
        date_selections = rng.integers(0, len(date_ids_arr), size=len(pub_indices))

        writer = self._create_parquet_writer("rel_published_on.parquet", schema)
        batch = []
        total_count = 0

        for i, pub_idx in enumerate(pub_indices):
            pub_id = _make_publication_doi(pub_idx)
            batch.append({"publication_doi": pub_id, "publish_date": date_ids_arr[date_selections[i]]})
            if len(batch) >= BATCH_SIZE:
                self._write_batch_to_parquet(writer, batch, fieldnames)
                total_count += len(batch)
                batch = []

        if batch:
            self._write_batch_to_parquet(writer, batch, fieldnames)
            total_count += len(batch)

        writer.close()
        self.counts["PUBLISHED_ON"] = total_count
        print(f"  Exported rel_published_on.parquet")
        print(f"Generated {total_count:,} PUBLISHED_ON relationships")

    def generate_all(self) -> None:
        print("=" * 60)
        print(f"Generating Scientific Research Papers Dataset")
        print(f"Using {self.config.workers} workers, output: {self.output_dir}")
        print("=" * 60)

        print("\n--- Generating and exporting nodes ---")
        self.generate_and_export_publish_dates()
        self.generate_and_export_journals()
        self.generate_and_export_authors()
        self.generate_and_export_concepts()
        self.generate_and_export_publications()
        self.generate_and_export_sections()

        print("\n--- Generating and exporting relationships ---")
        self.generate_and_export_section_part_of()
        self.generate_and_export_section_references()
        self.generate_and_export_publication_cites_paper()
        self.generate_and_export_concept_synonym_of()
        self.generate_and_export_concept_subfield_of()
        self.generate_and_export_publication_published_in()
        self.generate_and_export_publication_authored_by()
        self.generate_and_export_publication_published_on()

    def print_summary(self) -> None:
        print("\n" + "=" * 60)
        print("GENERATION SUMMARY")
        print("=" * 60)
        print("\nNodes:")
        total_nodes = 0
        for name in ["PublishDate", "Journal", "Author", "Concept", "Publication", "Section"]:
            if name in self.counts:
                print(f"  {name}: {self.counts[name]:>15,}")
                total_nodes += self.counts[name]
        print(f"  TOTAL: {total_nodes:>15,}")

        print("\nRelationships:")
        total_rels = 0
        for name in [
            "PART_OF",
            "*_REFERENCES",
            "CITES_PAPER",
            "SYNONYM_OF",
            "SUBFIELD_OF",
            "PUBLISHED_IN",
            "AUTHORED_BY",
            "PUBLISHED_ON",
        ]:
            if name in self.counts:
                print(f"  {name}: {self.counts[name]:>15,}")
                total_rels += self.counts[name]
        print(f"  TOTAL: {total_rels:>15,}")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic Scientific Research Papers graph dataset")
    parser.add_argument("-o", "--output", default="./generated_data", help="Output directory for Parquet files")
    parser.add_argument("-s", "--scale", type=float, default=1.0, help="Scale factor (0.1 for 10%%, 1.0 for full)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--workers", type=int, default=8, help="Number of parallel workers")
    parser.add_argument("--include-vectors", action="store_true", help="Include embedding vectors")

    args = parser.parse_args()

    config = GeneratorConfig(
        output_dir=args.output,
        scale=args.scale,
        seed=args.seed,
        workers=args.workers,
        include_vectors=args.include_vectors,
    )

    print("Scientific Research Papers Dataset Generator")
    print(f"  Output:  {config.output_dir}")
    print(f"  Scale:   {config.scale}x")
    print(f"  Seed:    {config.seed}")
    print(f"  Workers: {config.workers}")
    print()

    generator = DatasetGenerator(config)
    generator.generate_all()
    generator.print_summary()


if __name__ == "__main__":
    main()
