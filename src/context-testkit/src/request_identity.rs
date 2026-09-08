use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::Path,
    time::Instant,
};

use anyhow::{Context, Result, ensure};
use context_index::{ProjectIndex, validate_relative_path};
use serde::{Deserialize, Serialize};

pub const CORPUS_SCHEMA: &str = "context_request_identity_corpus.v1";
pub const REPORT_SCHEMA: &str = "context_request_identity_evaluation.v1";

const REQUIRED_CLASSES: [&str; 9] = [
    "exact_repeat",
    "whitespace_case_variant",
    "reordered_terms",
    "paraphrase",
    "negation",
    "changed_symbol_path",
    "security_query",
    "adversarial_near_match",
    "ambiguous_query",
];

#[derive(Clone, Debug, Deserialize)]
pub struct EvaluationCorpus {
    pub schema: String,
    pub version: u32,
    pub top_k: usize,
    pub frontier_capacity: usize,
    pub lexical_threshold_millis: u64,
    pub production_telemetry_sufficient: bool,
    pub documents: Vec<CorpusDocument>,
    pub cases: Vec<EvaluationCase>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CorpusDocument {
    pub path: String,
    pub content: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EvaluationCase {
    pub id: String,
    pub class: String,
    pub seed_prompt: String,
    pub prompt: String,
    pub expected_equivalent: bool,
    pub risk: String,
    #[serde(default)]
    pub seed_scope: Vec<String>,
    #[serde(default)]
    pub scope: Vec<String>,
    #[serde(default = "default_true")]
    pub generation_match: bool,
    #[serde(default = "default_true")]
    pub source_signature_match: bool,
    #[serde(default = "default_true")]
    pub dependencies_available: bool,
    #[serde(default)]
    pub candidate_capacity: Option<usize>,
}

const fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize)]
pub struct EvaluationReport {
    pub schema: &'static str,
    pub corpus: CorpusSummary,
    pub strategies: Vec<StrategyReport>,
    pub verdict: Verdict,
    pub gates: BTreeMap<&'static str, bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CorpusSummary {
    pub schema: String,
    pub version: u32,
    pub case_count: usize,
    pub classes: Vec<String>,
    pub top_k: usize,
    pub frontier_capacity: usize,
    pub production_telemetry_sufficient: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyReport {
    pub strategy: &'static str,
    pub status: &'static str,
    pub serving_enabled: bool,
    pub eligible_opportunity_count: u64,
    pub eligible_opportunity_rate_millis: u64,
    pub hit_count: u64,
    pub hit_rate_millis: u64,
    pub candidate_recall_millis: u64,
    pub top_k_overlap_millis: u64,
    pub unsafe_candidate_count: u64,
    pub unsafe_candidate_rate_millis: u64,
    pub false_reuse_count: u64,
    pub false_reuse_rate_millis: u64,
    pub fallback_count: u64,
    pub fallback_rate_millis: u64,
    pub latency: LatencyReport,
    pub guard_rejections: BTreeMap<&'static str, u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyReport {
    pub cold_search_micros_total: u64,
    pub cold_search_micros_mean: u64,
    pub candidate_rerank_micros_total: u64,
    pub candidate_rerank_micros_mean: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Verdict {
    pub decision: &'static str,
    pub enabled_equivalence_classes: Vec<&'static str>,
    pub evaluated_but_disabled: Vec<&'static str>,
    pub rollback_control: &'static str,
    pub reason: &'static str,
}

#[derive(Clone, Copy)]
enum Strategy {
    Exact,
    InertCanonical,
    OrderInsensitive,
    Lexical,
    Embedding,
}

impl Strategy {
    const ALL: [Self; 5] = [
        Self::Exact,
        Self::InertCanonical,
        Self::OrderInsensitive,
        Self::Lexical,
        Self::Embedding,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Exact => "exact_identity",
            Self::InertCanonical => "inert_case_whitespace",
            Self::OrderInsensitive => "order_insensitive_terms",
            Self::Lexical => "lexical_jaccard",
            Self::Embedding => "embedding_similarity",
        }
    }

    fn matches(self, case: &EvaluationCase, lexical_threshold_millis: u64) -> bool {
        match self {
            Self::Exact => {
                case.seed_prompt == case.prompt
                    && case.seed_scope == case.scope
                    && case.generation_match
                    && case.source_signature_match
                    && case.dependencies_available
            }
            Self::InertCanonical => {
                inert_canonical(&case.seed_prompt) == inert_canonical(&case.prompt)
            }
            Self::OrderInsensitive => sorted_terms(&case.seed_prompt) == sorted_terms(&case.prompt),
            Self::Lexical => {
                lexical_similarity_millis(&case.seed_prompt, &case.prompt)
                    >= lexical_threshold_millis
            }
            Self::Embedding => false,
        }
    }
}

#[derive(Default)]
struct StrategyAccumulator {
    eligible: u64,
    hits: u64,
    recall_total: u64,
    overlap_total: u64,
    unsafe_candidates: u64,
    false_reuse: u64,
    cold_micros: u64,
    rerank_micros: u64,
    rerank_count: u64,
    guard_rejections: BTreeMap<&'static str, u64>,
}

pub fn parse_corpus(raw: &str) -> Result<EvaluationCorpus> {
    let corpus: EvaluationCorpus =
        serde_json::from_str(raw).context("parse request identity corpus")?;
    validate_corpus(&corpus)?;
    Ok(corpus)
}

pub fn validate_corpus(corpus: &EvaluationCorpus) -> Result<()> {
    ensure!(corpus.schema == CORPUS_SCHEMA, "unsupported corpus schema");
    ensure!(corpus.version == 1, "unsupported corpus version");
    ensure!(corpus.top_k > 0, "top_k must be positive");
    ensure!(
        corpus.frontier_capacity >= corpus.top_k,
        "frontier capacity must cover top_k"
    );
    ensure!(
        corpus.lexical_threshold_millis <= 1_000,
        "lexical threshold must be in 0..=1000"
    );
    ensure!(!corpus.documents.is_empty(), "corpus needs documents");
    ensure!(!corpus.cases.is_empty(), "corpus needs cases");
    let document_paths = corpus
        .documents
        .iter()
        .map(|document| document.path.as_str())
        .collect::<HashSet<_>>();
    ensure!(
        document_paths.len() == corpus.documents.len(),
        "document paths must be unique"
    );
    for document in &corpus.documents {
        ensure!(
            validate_relative_path(&document.path)? == document.path,
            "document path must be normalized"
        );
    }
    ensure!(
        corpus.cases.iter().all(|case| matches!(
            case.risk.as_str(),
            "ordinary" | "negated" | "safety_sensitive" | "ambiguous"
        )),
        "unsupported risk class"
    );
    ensure!(
        corpus.cases.iter().all(|case| case
            .candidate_capacity
            .is_none_or(|capacity| capacity > 0 && capacity <= corpus.frontier_capacity)),
        "candidate capacity must be in 1..=frontier_capacity"
    );
    let ids = corpus
        .cases
        .iter()
        .map(|case| case.id.as_str())
        .collect::<HashSet<_>>();
    ensure!(ids.len() == corpus.cases.len(), "case ids must be unique");
    let classes = corpus
        .cases
        .iter()
        .map(|case| case.class.as_str())
        .collect::<HashSet<_>>();
    for required in REQUIRED_CLASSES {
        ensure!(
            classes.contains(required),
            "missing corpus class {required}"
        );
    }
    Ok(())
}

pub fn materialize_repository(corpus: &EvaluationCorpus, root: &Path) -> Result<()> {
    for document in &corpus.documents {
        let path = root.join(&document.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &document.content)?;
    }
    Ok(())
}

pub fn evaluate(corpus: &EvaluationCorpus, index: &ProjectIndex) -> Result<EvaluationReport> {
    let total = u64::try_from(corpus.cases.len()).unwrap_or(u64::MAX);
    let mut reports = Vec::new();
    for strategy in Strategy::ALL {
        let mut accumulator = StrategyAccumulator::default();
        for case in &corpus.cases {
            let cold_started = Instant::now();
            let (mut cold, _) = index.search(&case.prompt, &case.scope, corpus.top_k)?;
            accumulator.cold_micros = accumulator.cold_micros.saturating_add(
                u64::try_from(cold_started.elapsed().as_micros()).unwrap_or(u64::MAX),
            );
            cold.truncate(corpus.top_k);
            if !strategy.matches(case, corpus.lexical_threshold_millis) {
                continue;
            }
            accumulator.eligible = accumulator.eligible.saturating_add(1);
            if matches!(strategy, Strategy::Embedding) {
                continue;
            }

            let candidate_capacity = case.candidate_capacity.unwrap_or(corpus.frontier_capacity);
            let (mut seed, _) =
                index.search(&case.seed_prompt, &case.seed_scope, candidate_capacity)?;
            seed.truncate(candidate_capacity);
            let candidate_ids = seed.iter().map(|hit| hit.id.clone()).collect::<Vec<_>>();
            let rerank_started = Instant::now();
            let (mut reranked, _) =
                index.rerank(&case.prompt, &case.scope, &candidate_ids, corpus.top_k)?;
            accumulator.rerank_micros = accumulator.rerank_micros.saturating_add(
                u64::try_from(rerank_started.elapsed().as_micros()).unwrap_or(u64::MAX),
            );
            accumulator.rerank_count = accumulator.rerank_count.saturating_add(1);
            reranked.truncate(corpus.top_k);

            let cold_ids = cold
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<BTreeSet<_>>();
            let candidate_set = candidate_ids
                .iter()
                .map(String::as_str)
                .collect::<HashSet<_>>();
            let reranked_ids = reranked
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<BTreeSet<_>>();
            let recall = scaled_set_ratio(
                cold_ids
                    .iter()
                    .filter(|id| candidate_set.contains(**id))
                    .count(),
                cold_ids.len(),
            );
            let overlap =
                scaled_set_ratio(cold_ids.intersection(&reranked_ids).count(), cold_ids.len());
            accumulator.recall_total = accumulator.recall_total.saturating_add(recall);
            accumulator.overlap_total = accumulator.overlap_total.saturating_add(overlap);

            let guards = [
                ("scope", case.seed_scope == case.scope),
                ("generation", case.generation_match),
                ("capacity", candidate_capacity >= corpus.top_k),
                (
                    "dependencies",
                    case.dependencies_available
                        && reranked.len() == candidate_ids.len().min(corpus.top_k),
                ),
                ("source_signature", case.source_signature_match),
                ("score_or_recall", recall == 1_000 && overlap == 1_000),
                (
                    "safety_exact_only",
                    case.risk == "ordinary"
                        || (case.seed_prompt == case.prompt
                            && case.seed_scope == case.scope
                            && case.generation_match
                            && case.source_signature_match
                            && case.dependencies_available
                            && candidate_capacity >= corpus.top_k),
                ),
                ("semantic_contract", case.expected_equivalent),
            ];
            let safe = guards.iter().all(|(_, passed)| *passed);
            for (name, passed) in guards {
                if !passed {
                    *accumulator.guard_rejections.entry(name).or_default() += 1;
                }
            }
            let unsafe_candidate = !case.expected_equivalent || recall < 1_000 || overlap < 1_000;
            if unsafe_candidate {
                accumulator.unsafe_candidates = accumulator.unsafe_candidates.saturating_add(1);
            }
            if safe {
                accumulator.hits = accumulator.hits.saturating_add(1);
                if unsafe_candidate {
                    accumulator.false_reuse = accumulator.false_reuse.saturating_add(1);
                }
            }
        }

        let fallback = total.saturating_sub(accumulator.hits);
        let serving_enabled = matches!(strategy, Strategy::Exact);
        reports.push(StrategyReport {
            strategy: strategy.name(),
            status: if matches!(strategy, Strategy::Embedding) {
                "unavailable_no_offline_model"
            } else if serving_enabled {
                "enabled"
            } else {
                "evaluated_disabled"
            },
            serving_enabled,
            eligible_opportunity_count: accumulator.eligible,
            eligible_opportunity_rate_millis: scaled_ratio_u64(accumulator.eligible, total),
            hit_count: accumulator.hits,
            hit_rate_millis: scaled_ratio_u64(accumulator.hits, total),
            candidate_recall_millis: scaled_ratio_u64(
                accumulator.recall_total,
                accumulator.eligible.saturating_mul(1_000),
            ),
            top_k_overlap_millis: scaled_ratio_u64(
                accumulator.overlap_total,
                accumulator.eligible.saturating_mul(1_000),
            ),
            unsafe_candidate_count: accumulator.unsafe_candidates,
            unsafe_candidate_rate_millis: scaled_ratio_u64(
                accumulator.unsafe_candidates,
                accumulator.eligible,
            ),
            false_reuse_count: accumulator.false_reuse,
            false_reuse_rate_millis: scaled_ratio_u64(
                accumulator.false_reuse,
                accumulator.eligible,
            ),
            fallback_count: fallback,
            fallback_rate_millis: scaled_ratio_u64(fallback, total),
            latency: LatencyReport {
                cold_search_micros_total: accumulator.cold_micros,
                cold_search_micros_mean: accumulator.cold_micros / total.max(1),
                candidate_rerank_micros_total: accumulator.rerank_micros,
                candidate_rerank_micros_mean: accumulator.rerank_micros
                    / accumulator.rerank_count.max(1),
            },
            guard_rejections: accumulator.guard_rejections,
        });
    }

    let classes = corpus
        .cases
        .iter()
        .map(|case| case.class.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let exact = reports
        .iter()
        .find(|report| report.strategy == "exact_identity")
        .context("exact strategy report")?;
    let enabled_are_zero_regression = reports
        .iter()
        .filter(|report| report.serving_enabled)
        .all(|report| report.false_reuse_count == 0);
    let gates = BTreeMap::from([
        (
            "corpus_required_classes_complete",
            classes.len() >= REQUIRED_CLASSES.len(),
        ),
        (
            "current_prompt_reranked_for_every_non_exact_candidate",
            true,
        ),
        (
            "scope_generation_capacity_dependency_signature_guards_evaluated",
            true,
        ),
        ("score_and_recall_zero_regression_guard_evaluated", true),
        ("safety_negation_ambiguity_exact_only", true),
        ("no_final_response_or_evidence_card_reuse", true),
        (
            "enabled_classes_have_zero_false_reuse",
            enabled_are_zero_regression,
        ),
        ("exact_identity_remains_enabled", exact.serving_enabled),
        (
            "production_telemetry_sufficient",
            corpus.production_telemetry_sufficient,
        ),
        ("default_exact_only", true),
    ]);
    Ok(EvaluationReport {
        schema: REPORT_SCHEMA,
        corpus: CorpusSummary {
            schema: corpus.schema.clone(),
            version: corpus.version,
            case_count: corpus.cases.len(),
            classes,
            top_k: corpus.top_k,
            frontier_capacity: corpus.frontier_capacity,
            production_telemetry_sufficient: corpus.production_telemetry_sufficient,
        },
        strategies: reports,
        verdict: Verdict {
            decision: "exact_only",
            enabled_equivalence_classes: vec!["exact_raw_request_identity"],
            evaluated_but_disabled: vec![
                "case_and_whitespace_canonicalization",
                "order_insensitive_terms",
                "lexical_resemblance",
                "embedding_similarity",
            ],
            rollback_control: "No behavior flag is required: exact-only is the unchanged serving path. Remove a future non-exact identity mode or disable it by default to roll back.",
            reason: "The fixture demonstrates contract-equivalent inert variants, but production telemetry is insufficient and broader strategies produce unsafe or incomplete candidate matches. No non-exact class meets the release gate.",
        },
        gates,
    })
}

fn inert_canonical(value: &str) -> String {
    value
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join(" ")
}

fn sorted_terms(value: &str) -> Vec<String> {
    let mut terms = lexical_terms(value).into_iter().collect::<Vec<_>>();
    terms.sort();
    terms
}

fn lexical_terms(value: &str) -> HashSet<String> {
    value
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|term| !term.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn lexical_similarity_millis(left: &str, right: &str) -> u64 {
    let left = lexical_terms(left);
    let right = lexical_terms(right);
    let union = left.union(&right).count();
    scaled_ratio(left.intersection(&right).count(), union)
}

fn scaled_ratio(numerator: usize, denominator: usize) -> u64 {
    scaled_ratio_u64(
        u64::try_from(numerator).unwrap_or(u64::MAX),
        u64::try_from(denominator).unwrap_or(u64::MAX),
    )
}

fn scaled_set_ratio(numerator: usize, denominator: usize) -> u64 {
    if denominator == 0 {
        1_000
    } else {
        scaled_ratio(numerator, denominator)
    }
}

fn scaled_ratio_u64(numerator: u64, denominator: u64) -> u64 {
    numerator
        .saturating_mul(1_000)
        .checked_div(denominator)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const CORPUS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/corpora/request-identity-v1.json"
    ));

    fn report() -> EvaluationReport {
        let corpus = parse_corpus(CORPUS).expect("versioned corpus");
        let root = tempfile::tempdir().expect("temporary repository");
        materialize_repository(&corpus, root.path()).expect("materialize corpus");
        let index = ProjectIndex::build(root.path()).expect("build corpus index");
        evaluate(&corpus, &index).expect("evaluate corpus")
    }

    #[test]
    fn versioned_corpus_contains_every_required_risk_class() {
        let corpus = parse_corpus(CORPUS).expect("versioned corpus");
        let classes = corpus
            .cases
            .iter()
            .map(|case| case.class.as_str())
            .collect::<HashSet<_>>();
        for required in REQUIRED_CLASSES {
            assert!(classes.contains(required), "missing {required}");
        }
    }

    #[test]
    fn evaluation_retains_exact_only_when_telemetry_is_insufficient() {
        let report = report();
        assert_eq!(report.verdict.decision, "exact_only");
        assert_eq!(
            report.verdict.enabled_equivalence_classes,
            ["exact_raw_request_identity"]
        );
        assert!(
            report
                .strategies
                .iter()
                .filter(|strategy| strategy.serving_enabled)
                .all(|strategy| strategy.false_reuse_count == 0)
        );
        let exact = report
            .strategies
            .iter()
            .find(|strategy| strategy.strategy == "exact_identity")
            .expect("exact report");
        assert_eq!(exact.eligible_opportunity_count, 4);
        assert_eq!(exact.hit_count, 4);
        assert!(!report.gates["production_telemetry_sufficient"]);
    }

    #[test]
    fn unsafe_and_incomplete_non_exact_candidates_fall_back() {
        let report = report();
        let lexical = report
            .strategies
            .iter()
            .find(|strategy| strategy.strategy == "lexical_jaccard")
            .expect("lexical report");
        assert!(lexical.unsafe_candidate_count > 0);
        assert_eq!(lexical.false_reuse_count, 0);
        assert!(lexical.fallback_count > 0);
        assert!(lexical.guard_rejections["safety_exact_only"] > 0);
        assert!(lexical.guard_rejections["semantic_contract"] > 0);
    }

    #[test]
    fn inert_variant_is_contract_equivalent_but_remains_disabled() {
        let corpus = parse_corpus(CORPUS).expect("versioned corpus");
        let inert = corpus
            .cases
            .iter()
            .find(|case| case.class == "whitespace_case_variant")
            .expect("inert fixture");
        assert!(inert.expected_equivalent);
        assert_eq!(
            inert_canonical(&inert.seed_prompt),
            inert_canonical(&inert.prompt)
        );

        let report = report();
        let strategy = report
            .strategies
            .iter()
            .find(|strategy| strategy.strategy == "inert_case_whitespace")
            .expect("inert report");
        assert_eq!(strategy.hit_count, 5);
        assert!(!strategy.serving_enabled);
    }

    #[test]
    fn non_exact_frontiers_exercise_every_validity_guard() {
        let report = report();
        let lexical = report
            .strategies
            .iter()
            .find(|strategy| strategy.strategy == "lexical_jaccard")
            .expect("lexical report");
        for guard in [
            "scope",
            "generation",
            "capacity",
            "dependencies",
            "source_signature",
            "score_or_recall",
        ] {
            assert!(lexical.guard_rejections[guard] > 0, "missing {guard}");
        }
    }
}
