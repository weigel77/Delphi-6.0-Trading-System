"""Apollo Gate 3 candidate generation for structured SPX 1DTE mode outputs."""

from __future__ import annotations

from collections import Counter
import logging
from typing import Any, Dict, List

from config import AppConfig, get_app_config


LOGGER = logging.getLogger(__name__)


class ApolloCandidateService:
    """Build deterministic SPX 1DTE put credit spread recommendations for three Apollo modes."""

    TITLE = "Apollo Gate 3 -- Trade Candidates"
    TARGET_WIDTHS = (5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0)
    MIN_NET_CREDIT = 1.00
    SHARED_MIN_RISK_EFFICIENCY = 0.05
    AGGRESSIVE_MIN_RISK_EFFICIENCY = SHARED_MIN_RISK_EFFICIENCY
    STANDARD_MIN_RISK_EFFICIENCY = SHARED_MIN_RISK_EFFICIENCY
    FORTRESS_MIN_RISK_EFFICIENCY = SHARED_MIN_RISK_EFFICIENCY
    MIN_CREDIT_TO_WIDTH = 0.05
    MAX_SHORT_BID_ASK_WIDTH = 1.20
    REALISTIC_MAX_LOSS_FACTOR = 0.60
    FORTRESS_BASE_WIDTH = 15.0
    FORTRESS_BASE_CONTRACTS = 10
    FORTRESS_NEIGHBORHOOD_RANGE = 30.0
    FORTRESS_ALLOWED_WIDTHS = (10.0, 15.0, 20.0)
    FORTRESS_ALLOWED_CONTRACTS = tuple(range(5, 21))
    FORTRESS_MAX_LOSS_CAP_DOLLARS = 15000.0
    MODE_MAX_LOSS_CAPS = {
        "fortress": 0.10,
        "standard": 0.15,
        "aggressive": 0.20,
    }
    def __init__(self, config: AppConfig | None = None) -> None:
        self.config = config or get_app_config()
        self.account_value = float(self.config.apollo_account_value or 135000.0)

    def build_trade_candidates(
        self,
        option_chain: Dict[str, Any],
        structure: Dict[str, Any],
        macro: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Return one deterministic candidate for each Apollo mode."""
        if not option_chain.get("success"):
            return self._build_empty_result(
                option_chain=option_chain,
                status="Stand Aside",
                status_class="poor",
                message="Trade candidates are unavailable because the SPX option chain could not be retrieved. No trade execution is performed.",
            )

        puts = self._normalize_contracts(option_chain.get("puts") or [])
        calls = self._normalize_contracts(option_chain.get("calls") or [])
        spot = self._coerce_float(option_chain.get("underlying_price"))
        expected_move = self._estimate_expected_move(puts=puts, calls=calls, spot=spot)

        if not puts or spot is None:
            return self._build_empty_result(
                option_chain=option_chain,
                status="Stand Aside",
                status_class="poor",
                message="Trade candidates could not be scored because the SPX chain did not include enough put-side detail. No trade execution is performed.",
                expected_move=expected_move,
                expected_move_range=self._build_expected_move_range(spot, expected_move),
                underlying_price=spot,
            )

        if expected_move is None or expected_move <= 0:
            return self._build_empty_result(
                option_chain=option_chain,
                status="Stand Aside",
                status_class="poor",
                message="Trade candidates could not be scored because Apollo could not derive the expected move required for Gate 3 mode selection. No trade execution is performed.",
                expected_move=expected_move,
                expected_move_range=self._build_expected_move_range(spot, expected_move),
                underlying_price=spot,
            )

        structure_grade = self._normalize_structure_grade(structure.get("final_grade") or structure.get("grade"))
        base_structure_grade = self._normalize_structure_grade(structure.get("base_grade") or structure_grade)
        rsi_modifier_applied = bool(structure.get("rsi_modifier_applied"))
        rsi_modifier_label = str(structure.get("rsi_modifier_label") or "None")
        macro_grade = self._normalize_macro_grade(macro.get("grade"))
        macro_present = macro_grade in {"Minor", "Major"}
        standard_barrier_points = expected_move * 1.5
        standard_barrier_strike = self._floor_to_valid_strike(spot - standard_barrier_points, [item["strike"] for item in puts])
        full_size_barrier_points = expected_move * 2.0 if (macro_present or structure_grade == "Poor") else None

        strike_lookup = {
            round(contract["strike"], 2): contract
            for contract in puts
            if contract.get("strike") is not None
        }
        spread_pool: List[Dict[str, Any]] = []
        for short_put in sorted(puts, key=lambda item: item["strike"], reverse=True):
            if short_put["strike"] >= spot:
                continue
            for width in self.TARGET_WIDTHS:
                long_put = strike_lookup.get(round(short_put["strike"] - width, 2))
                if long_put is None:
                    continue
                spread_pool.append(
                    self._build_spread_snapshot(
                        short_put=short_put,
                        long_put=long_put,
                        width=width,
                        spot=spot,
                        expected_move=expected_move,
                    )
                )

        mode_outputs: List[Dict[str, Any]] = []
        evaluated_spread_details: List[Dict[str, Any]] = []
        aggregate_rejections: Counter[str] = Counter()
        mode_summaries: List[Dict[str, Any]] = []
        selected_candidates: Dict[str, Dict[str, Any] | None] = {}

        for mode in self._build_mode_configs(
            spot=spot,
            expected_move=expected_move,
            structure_grade=structure_grade,
            macro_grade=macro_grade,
        ):
            standard_reference = selected_candidates.get("standard") if mode["key"] == "fortress" else None
            selected_candidate, mode_logs, rejection_counts = self._select_mode_candidate(
                mode=mode,
                spread_pool=spread_pool,
                structure=structure,
                macro=macro,
                standard_reference=standard_reference,
            )
            evaluated_spread_details.extend(mode_logs)
            for reason, count in rejection_counts.items():
                aggregate_rejections[f"{mode['label']}: {reason}"] += count

            if selected_candidate is None:
                default_policy = mode.get("strict_policy") or mode.get("fallback_policy") or {}
                mode_output = {
                    "mode_key": mode["key"],
                    "mode_label": mode["label"],
                    "mode_descriptor": mode["descriptor"],
                    "available": False,
                    "no_trade_message": self._build_no_trade_message(mode, rejection_counts),
                    "active_rule_set": default_policy.get("active_rule_set") or mode["rule_set_label"],
                    "expected_move": round(expected_move, 2),
                    "expected_move_used": round(expected_move, 2),
                    "expected_move_source": "atm_straddle_estimate",
                    "expected_move_1_5x_threshold": round(expected_move * 1.5, 2),
                    "expected_move_2x_threshold": round(expected_move * 2.0, 2),
                    "target_em_multiple": default_policy.get("target_em_multiple"),
                    "applied_em_multiple_floor": default_policy.get("applied_em_multiple_floor"),
                    "percent_floor": default_policy.get("percent_floor"),
                    "percent_floor_points": round(float(default_policy.get("percent_floor_points") or 0.0), 2) if default_policy else None,
                    "em_floor_points": round(float(default_policy.get("em_floor_points") or 0.0), 2) if default_policy else None,
                    "hybrid_distance_threshold": round(float(default_policy.get("distance_threshold") or 0.0), 2) if default_policy else None,
                    "boundary_binding_source": default_policy.get("boundary_binding_source") or "—",
                    "boundary_rule_used": default_policy.get("boundary_rule_used") or mode["rule_set_label"],
                    "pass_type": default_policy.get("pass_type") or "strict pass",
                    "pass_type_label": default_policy.get("pass_type_label") or default_policy.get("pass_type") or "Strict Pass",
                    "fallback_used": bool(default_policy.get("fallback_used")),
                    "fallback_rule_name": default_policy.get("fallback_rule_name") or "",
                    "actual_distance_to_short": None,
                    "actual_em_multiple": None,
                    "diagnostics": self._build_no_trade_diagnostics(mode, default_policy, rejection_counts),
                    "rationale": [],
                    "exit_plan": self._build_exit_plan(short_strike=None, long_strike=None),
                    "risk_cap_dollars": self._risk_cap_dollars(mode["key"]),
                    "risk_cap_status": "No valid trade under this mode.",
                    "risk_cap_adjusted": False,
                    "original_contract_size": None,
                    "account_risk_percent": None,
                    "realistic_max_loss": None,
                }
            else:
                selected_candidates[mode["key"]] = selected_candidate
                mode_output = {
                    **selected_candidate,
                    "mode_key": mode["key"],
                    "mode_label": mode["label"],
                    "mode_descriptor": mode["descriptor"],
                    "available": bool(selected_candidate.get("available", True)),
                    "no_trade_message": str(selected_candidate.get("no_trade_message", "")),
                }
            mode_outputs.append(mode_output)
            mode_summaries.append(
                {
                    "label": mode["label"],
                    "rule_set": mode["rule_set_label"],
                    "selected_short_strike": mode_output.get("short_strike"),
                    "available": mode_output.get("available", False),
                }
            )

        valid_mode_count = sum(1 for item in mode_outputs if item.get("available"))
        status = "Ready" if valid_mode_count else "Stand Aside"
        status_class = "good" if valid_mode_count else "poor"
        if valid_mode_count and (macro_present or structure_grade in {"Neutral", "Poor"}):
            status = "Caution"
            status_class = "neutral"

        message = (
            "Apollo produced three Gate 3 mode outputs with distinct risk philosophies. No trade execution is performed."
            if valid_mode_count
            else "No Gate 3 mode produced a valid SPX 1DTE trade for this expiration. No trade execution is performed."
        )

        return {
            "title": self.TITLE,
            "success": bool(valid_mode_count),
            "status": status,
            "status_class": status_class,
            "message": message,
            "candidate_count": len(mode_outputs),
            "count_label": "3 Modes",
            "valid_mode_count": valid_mode_count,
            "underlying_price": round(spot, 2),
            "expected_move": round(expected_move, 2),
            "expected_move_range": self._build_expected_move_range(spot, expected_move),
            "expiration_date": option_chain.get("expiration_date"),
            "candidates": mode_outputs,
            "diagnostics": {
                "evaluated_spreads": len(evaluated_spread_details),
                "qualified_spreads": sum(1 for item in evaluated_spread_details if item.get("reject_reason") == "Passed"),
                "selected_spreads": valid_mode_count,
                "rejected_spreads": sum(1 for item in evaluated_spread_details if item.get("reject_reason") != "Passed"),
                "required_distance_mode": "1.5x expected move baseline",
                "baseline_distance_points": round(standard_barrier_points, 2),
                "baseline_max_short_strike": round(standard_barrier_strike, 2) if standard_barrier_strike is not None else None,
                "expected_move": round(expected_move, 2),
                "expected_move_1_5x_threshold": round(expected_move * 1.5, 2),
                "expected_move_2x_threshold": round(expected_move * 2.0, 2),
                "active_barrier_mode": "1.5x expected move baseline",
                "active_barrier_points": round(standard_barrier_points, 2),
                "macro_modifier_applied": "Yes" if macro_present else "No",
                "structure_modifier_applied": "Yes" if structure_grade == "Poor" else "No",
                "structure_grade_used": structure_grade,
                "base_structure_grade": base_structure_grade,
                "rsi_modifier_applied": "Yes" if rsi_modifier_applied else "No",
                "rsi_modifier_label": rsi_modifier_label,
                "recommended_contract_size_framework": "Standard mode stays at 8 unless macro or final structure = Poor keeps size at 5 because distance is below 2x expected move. Aggressive stays 8 by default and can scale to 10. Fortress stays 8 only with Good final structure and no macro; otherwise 5.",
                "account_value": round(self.account_value, 2),
                "top_rejections": [
                    {"label": label, "count": count}
                    for label, count in aggregate_rejections.most_common(6)
                ],
                "mode_summaries": mode_summaries,
                "evaluated_spread_details": evaluated_spread_details,
                "full_size_barrier_points": round(full_size_barrier_points, 2) if full_size_barrier_points is not None else None,
            },
        }

    def _build_mode_configs(
        self,
        spot: float,
        expected_move: float,
        structure_grade: str,
        macro_grade: str,
    ) -> List[Dict[str, Any]]:
        macro_present = macro_grade in {"Minor", "Major"}
        fortress_em_multiple = self._resolve_fortress_em_multiple(
            standard_em_multiple=1.8,
            structure_grade=structure_grade,
            macro_grade=macro_grade,
        )
        return [
            {
                "key": "standard",
                "label": "Standard (Apollo Core)",
                "descriptor": "Balanced risk using Apollo core rules",
                "rule_set_label": "Strict 1.80x EM with a fallback to 1.60x EM, using the shared Apollo minimum risk efficiency gate",
                "delta_preferred": 0.12,
                "delta_max": 0.15,
                "prioritize": "balanced",
                "size_default": 8,
                "size_restricted": 5,
                "full_size_threshold": expected_move * 2.0 if (macro_present or structure_grade == "Poor") else None,
                "macro_present": macro_present,
                "macro_grade": macro_grade,
                "structure_grade": structure_grade,
                "strict_policy": self._build_mode_policy(
                    mode_key="standard",
                    policy_name="strict",
                    expected_move=expected_move,
                    spot=spot,
                    target_em_multiple=1.8,
                    percent_floor=None,
                    min_credit=1.0,
                    min_risk_efficiency=self.STANDARD_MIN_RISK_EFFICIENCY,
                    fallback_used=False,
                ),
                "fallback_policy": self._build_mode_policy(
                    mode_key="standard",
                    policy_name="fallback",
                    expected_move=expected_move,
                    spot=spot,
                    target_em_multiple=1.6,
                    percent_floor=None,
                    min_credit=1.0,
                    min_risk_efficiency=self.STANDARD_MIN_RISK_EFFICIENCY,
                    fallback_used=True,
                    fallback_rule_name="standard_1.6x_em_fallback",
                ),
            },
            {
                "key": "aggressive",
                "label": "Aggressive (Yield Mode)",
                "descriptor": "Higher premium with controlled risk relaxation",
                "rule_set_label": "Strict 1.50x EM with the shared Apollo minimum risk efficiency gate and no fallback relaxation",
                "delta_preferred": 0.20,
                "delta_max": 0.25,
                "prioritize": "premium",
                "size_default": 8,
                "size_upside": 10,
                "allow_scale_up": (not macro_present and structure_grade in {"Good", "Neutral"}),
                "macro_present": macro_present,
                "macro_grade": macro_grade,
                "structure_grade": structure_grade,
                "strict_policy": self._build_mode_policy(
                    mode_key="aggressive",
                    policy_name="strict",
                    expected_move=expected_move,
                    spot=spot,
                    target_em_multiple=1.5,
                    percent_floor=None,
                    min_credit=1.0,
                    min_risk_efficiency=self.AGGRESSIVE_MIN_RISK_EFFICIENCY,
                    fallback_used=False,
                ),
                "fallback_policy": None,
            },
            {
                "key": "fortress",
                "label": "Fortress (Max Safety)",
                "descriptor": "Maximum distance and lowest probability of touch",
                "rule_set_label": (
                    f"Deterministic Fortress engine: first qualifying short at Standard EM x 1.10 with macro / structure add-ons ({fortress_em_multiple:.2f}x EM fallback), "
                    "base 15-point width with 10 contracts at short bid / long ask, then neighborhood optimization by credit efficiency"
                ),
                "delta_preferred": 0.08,
                "delta_max": 0.10,
                "prioritize": "distance",
                "size_default": self.FORTRESS_BASE_CONTRACTS,
                "size_restricted": self.FORTRESS_BASE_CONTRACTS,
                "macro_present": macro_present,
                "macro_grade": macro_grade,
                "structure_grade": structure_grade,
                "strict_policy": self._build_mode_policy(
                    mode_key="fortress",
                    policy_name="strict",
                    expected_move=expected_move,
                    spot=spot,
                    target_em_multiple=fortress_em_multiple,
                    percent_floor=None,
                    min_credit=None,
                    min_risk_efficiency=None,
                    pass_type="fortress",
                    pass_type_label="Fortress",
                    fallback_used=False,
                ),
                "fallback_policy": None,
            },
        ]

    def _build_mode_policy(
        self,
        *,
        mode_key: str,
        policy_name: str,
        expected_move: float,
        spot: float,
        target_em_multiple: float,
        percent_floor: float | None,
        min_credit: float | None,
        min_risk_efficiency: float | None = None,
        pass_type: str | None = None,
        pass_type_label: str | None = None,
        fallback_used: bool,
        fallback_rule_name: str = "",
    ) -> Dict[str, Any]:
        percent_floor_points = None
        em_floor_points = expected_move * target_em_multiple
        distance_threshold = em_floor_points
        binding_source = "EM Floor"
        resolved_pass_type = pass_type or f"{mode_key}_{policy_name}"
        resolved_pass_type_label = pass_type_label or ("Fallback Pass" if fallback_used else "Strict Pass")
        rule_label = f"{target_em_multiple:.2f}x EM"
        return {
            "mode_key": mode_key,
            "policy_name": policy_name,
            "pass_type": resolved_pass_type,
            "pass_type_label": resolved_pass_type_label,
            "fallback_used": fallback_used,
            "fallback_rule_name": fallback_rule_name,
            "target_em_multiple": target_em_multiple,
            "applied_em_multiple_floor": target_em_multiple,
            "min_credit": min_credit,
            "min_risk_efficiency": min_risk_efficiency,
            "percent_floor": percent_floor,
            "percent_floor_points": percent_floor_points,
            "em_floor_points": em_floor_points,
            "distance_threshold": distance_threshold,
            "distance_threshold_label": rule_label,
            "boundary_binding_source": binding_source,
            "boundary_rule_used": f"{target_em_multiple:.2f}x expected move",
            "active_rule_set": (
                f"{resolved_pass_type_label} using {rule_label}"
                if not fallback_used
                else f"{resolved_pass_type_label} using {rule_label} ({fallback_rule_name})"
            ),
        }

    def _select_mode_candidate(
        self,
        mode: Dict[str, Any],
        spread_pool: List[Dict[str, Any]],
        structure: Dict[str, Any],
        macro: Dict[str, Any],
        standard_reference: Dict[str, Any] | None = None,
    ) -> tuple[Dict[str, Any] | None, List[Dict[str, Any]], Counter[str]]:
        if mode["key"] == "fortress":
            return self._select_fortress_candidate(
                mode=mode,
                policy=mode.get("strict_policy") or {},
                spread_pool=spread_pool,
                standard_reference=standard_reference,
            )

        evaluated_logs: List[Dict[str, Any]] = []
        rejection_counts: Counter[str] = Counter()
        invalid_candidates: List[Dict[str, Any]] = []
        for policy in [mode.get("strict_policy"), mode.get("fallback_policy")]:
            if not policy:
                continue
            policy_rejection_counts: Counter[str] = Counter()
            policy_logs: List[Dict[str, Any]] = []
            ordered_spreads = self._order_spread_pool_for_mode(mode, spread_pool)

            if mode["key"] == "fortress":
                width_buckets = sorted({float(spread["width"]) for spread in ordered_spreads})
                for width in width_buckets:
                    qualified: List[Dict[str, Any]] = []
                    for spread in [item for item in ordered_spreads if float(item["width"]) == float(width)]:
                        candidate, rejection_reason, log = self._evaluate_mode_candidate(
                            mode=mode,
                            policy=policy,
                            spread=spread,
                            spread_pool=spread_pool,
                            structure=structure,
                            macro=macro,
                            standard_reference=standard_reference,
                        )
                        policy_logs.append(log)
                        LOGGER.info("Apollo Gate 3 %s evaluation | %s", mode["key"], log)
                        if candidate is None:
                            policy_rejection_counts[rejection_reason or "Rejected by mode filter"] += 1
                            continue
                        if not candidate.get("available", True):
                            invalid_candidates.append(candidate)
                            policy_rejection_counts[rejection_reason or candidate.get("risk_cap_status") or "Rejected by mode filter"] += 1
                            continue
                        qualified.append(candidate)

                    if qualified:
                        evaluated_logs.extend(policy_logs)
                        rejection_counts.update(policy_rejection_counts)
                        qualified.sort(key=self._fortress_candidate_sort_key, reverse=True)
                        best = qualified[0]
                        best["diagnostics"].append(
                            f"Fortress search order: 2.00x EM safety first, then {int(width):d}-point width candidates, then farther qualifying strikes, then executable-premium sizing inside the total max-loss cap."
                        )
                        best["diagnostics"].append(self._format_rejection_summary(policy_rejection_counts))
                        return best, evaluated_logs, rejection_counts

                evaluated_logs.extend(policy_logs)
                rejection_counts.update(policy_rejection_counts)
                continue

            qualified: List[Dict[str, Any]] = []

            for spread in ordered_spreads:
                candidate, rejection_reason, log = self._evaluate_mode_candidate(
                    mode=mode,
                    policy=policy,
                    spread=spread,
                    spread_pool=spread_pool,
                    structure=structure,
                    macro=macro,
                    standard_reference=standard_reference,
                )
                policy_logs.append(log)
                LOGGER.info("Apollo Gate 3 %s evaluation | %s", mode["key"], log)
                if candidate is None:
                    policy_rejection_counts[rejection_reason or "Rejected by mode filter"] += 1
                    continue
                if not candidate.get("available", True):
                    invalid_candidates.append(candidate)
                    policy_rejection_counts[rejection_reason or candidate.get("risk_cap_status") or "Rejected by mode filter"] += 1
                    continue
                qualified.append(candidate)

            evaluated_logs.extend(policy_logs)
            rejection_counts.update(policy_rejection_counts)

            if not qualified:
                continue

            qualified.sort(key=lambda item: self._mode_sort_key(mode, item), reverse=True)
            best = qualified[0]
            best["diagnostics"].append(self._format_rejection_summary(policy_rejection_counts))
            return best, evaluated_logs, rejection_counts

        if invalid_candidates:
            invalid_candidates.sort(key=lambda item: self._mode_sort_key(mode, item), reverse=True)
            best_invalid = invalid_candidates[0]
            best_invalid["no_trade_message"] = self._build_no_trade_message(mode, rejection_counts)
            best_invalid["diagnostics"].append(self._format_rejection_summary(rejection_counts))
            return best_invalid, evaluated_logs, rejection_counts

        return None, evaluated_logs, rejection_counts

    def _select_fortress_candidate(
        self,
        *,
        mode: Dict[str, Any],
        policy: Dict[str, Any],
        spread_pool: List[Dict[str, Any]],
        standard_reference: Dict[str, Any] | None,
    ) -> tuple[Dict[str, Any] | None, List[Dict[str, Any]], Counter[str]]:
        rejection_counts: Counter[str] = Counter()
        evaluation_logs: List[Dict[str, Any]] = []
        active_policy = self._build_fortress_policy(
            base_policy=policy,
            mode=mode,
            spread_pool=spread_pool,
            standard_reference=standard_reference,
        )
        standard_short_strike = (
            float(standard_reference.get("short_strike"))
            if isinstance(standard_reference, dict) and standard_reference.get("short_strike") is not None
            else None
        )
        candidate_base_spreads = [
            spread
            for spread in sorted(
                spread_pool,
                key=lambda item: (-float(item["short_put"]["strike"]), float(item["width"])),
            )
            if float(spread.get("width") or 0.0) == self.FORTRESS_BASE_WIDTH
            and max(float(spread.get("executable_credit") or 0.0), 0.0) > 0.0
            and (standard_short_strike is None or float(spread["short_put"]["strike"]) <= standard_short_strike)
        ]
        qualifying_base_spreads = [
            spread for spread in candidate_base_spreads if self._fortress_spread_qualifies(spread=spread, policy=active_policy)
        ]

        baseline_meets_em = bool(qualifying_base_spreads)
        if not qualifying_base_spreads and candidate_base_spreads:
            base_spread = candidate_base_spreads[0]
            rejection_counts[f"Failed {active_policy.get('boundary_rule_used') or 'Fortress EM rule'} distance rule"] += 1
        elif qualifying_base_spreads:
            base_spread = qualifying_base_spreads[0]
        else:
            fallback_candidates = [
                spread
                for spread in sorted(spread_pool, key=lambda item: (-float(item["distance_points"]), float(item["width"])))
                if max(float(spread.get("executable_credit") or 0.0), 0.0) > 0.0
            ]
            if not fallback_candidates:
                return None, evaluation_logs, rejection_counts
            base_spread = fallback_candidates[0]
            baseline_meets_em = False
            rejection_counts[f"Failed {active_policy.get('boundary_rule_used') or 'Fortress EM rule'} distance rule"] += 1

        base_candidate = self._build_fortress_candidate(
            mode=mode,
            policy=active_policy,
            spread=base_spread,
            contracts=self.FORTRESS_BASE_CONTRACTS,
            selected_variant="base",
            selection_note=(
                "Base Fortress position uses the first qualifying short strike with a fixed 15-point width and 10 contracts."
                if baseline_meets_em
                else "Base Fortress position fell back to the nearest executable 15-point structure because no 15-point spread cleared the anchored Fortress EM threshold."
            ),
        )
        base_candidate["baseline_meets_fortress_em"] = baseline_meets_em
        evaluation_logs.append(
            self._build_fortress_evaluation_log(
                mode=mode,
                policy=active_policy,
                candidate=base_candidate,
                reject_reason="Passed (base)",
            )
        )

        optimized_candidates: List[Dict[str, Any]] = []
        base_short_strike = float(base_candidate["short_strike"])
        for spread in spread_pool:
            width = float(spread.get("width") or 0.0)
            short_strike = float(spread["short_put"]["strike"])
            if width not in self.FORTRESS_ALLOWED_WIDTHS:
                continue
            if abs(short_strike - base_short_strike) > self.FORTRESS_NEIGHBORHOOD_RANGE:
                continue
            if not self._fortress_spread_qualifies(
                spread=spread,
                policy=active_policy,
                standard_short_strike=standard_short_strike,
            ):
                rejection_counts[f"Failed {active_policy.get('boundary_rule_used') or 'Fortress EM rule'} distance rule"] += 1
                continue
            for contracts in self.FORTRESS_ALLOWED_CONTRACTS:
                candidate = self._build_fortress_candidate(
                    mode=mode,
                    policy=active_policy,
                    spread=spread,
                    contracts=contracts,
                    selected_variant="optimized",
                    selection_note="Candidate evaluated inside the Fortress neighborhood optimizer.",
                )
                if float(candidate["max_loss"] or 0.0) > self.FORTRESS_MAX_LOSS_CAP_DOLLARS:
                    rejection_counts["Rejected: exceeds $15,000 max loss cap"] += 1
                    evaluation_logs.append(
                        self._build_fortress_evaluation_log(
                            mode=mode,
                            policy=active_policy,
                            candidate=candidate,
                            reject_reason="Rejected: exceeds $15,000 max loss cap",
                        )
                    )
                    continue
                optimized_candidates.append(candidate)
                evaluation_logs.append(
                    self._build_fortress_evaluation_log(
                        mode=mode,
                        policy=active_policy,
                        candidate=candidate,
                        reject_reason="Passed",
                    )
                )

        superior_candidates = [
            item
            for item in optimized_candidates
            if not (
                float(item["short_strike"]) == float(base_candidate["short_strike"])
                and float(item["long_strike"]) == float(base_candidate["long_strike"])
                and float(item["width"]) == float(base_candidate["width"])
                and int(item["recommended_contract_size"]) == int(base_candidate["recommended_contract_size"])
            )
        ]
        best_optimized = max(superior_candidates, key=self._fortress_selection_priority_key, default=None)

        final_candidate = base_candidate
        optimized_summary = None
        if best_optimized is not None:
            optimized_summary = self._build_fortress_candidate_summary(best_optimized)
            if self._fortress_selection_priority_key(best_optimized) > self._fortress_selection_priority_key(base_candidate):
                final_candidate = dict(best_optimized)
                final_candidate["selection_variant"] = "optimized"
                final_candidate["diagnostics"] = list(final_candidate.get("diagnostics") or [])
                final_candidate["diagnostics"].insert(0, "Optimized Fortress candidate replaced the deterministic base candidate.")
            else:
                final_candidate = dict(base_candidate)
                final_candidate["diagnostics"] = list(final_candidate.get("diagnostics") or [])
                final_candidate["diagnostics"].insert(0, "Base Fortress candidate was retained because no optimized neighbor ranked higher by credit efficiency, total premium, then max loss.")
        else:
            final_candidate = dict(base_candidate)
            final_candidate["diagnostics"] = list(final_candidate.get("diagnostics") or [])
            final_candidate["diagnostics"].insert(0, "Base Fortress candidate was used because no superior optimized candidate qualified inside the neighborhood search.")

        final_candidate["base_candidate"] = self._build_fortress_candidate_summary(base_candidate)
        final_candidate["optimized_candidate"] = optimized_summary
        final_candidate["pricing_basis_note"] = "Short = Bid, Long = Ask"
        final_candidate["available"] = True
        return final_candidate, evaluation_logs, rejection_counts

    def _fortress_spread_qualifies(
        self,
        *,
        spread: Dict[str, Any],
        policy: Dict[str, Any],
        standard_short_strike: float | None = None,
    ) -> bool:
        executable_credit = float(spread.get("executable_credit") or 0.0)
        return bool(
            executable_credit > 0
            and float(spread.get("distance_points") or 0.0) >= float(policy.get("distance_threshold") or 0.0)
            and (standard_short_strike is None or float(spread["short_put"]["strike"]) <= standard_short_strike)
        )

    def _build_fortress_policy(
        self,
        *,
        base_policy: Dict[str, Any],
        mode: Dict[str, Any],
        spread_pool: List[Dict[str, Any]],
        standard_reference: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        if not spread_pool:
            return dict(base_policy)
        expected_move = float(spread_pool[0].get("expected_move") or 0.0)
        spot = float(spread_pool[0].get("spot") or 0.0)
        standard_em_multiple = float(
            (standard_reference or {}).get("em_multiple")
            or (standard_reference or {}).get("actual_em_multiple")
            or (standard_reference or {}).get("target_em_multiple")
            or 1.8
        )
        fortress_em_multiple = self._resolve_fortress_em_multiple(
            standard_em_multiple=standard_em_multiple,
            structure_grade=str(mode.get("structure_grade") or ""),
            macro_grade=str(mode.get("macro_grade") or ""),
        )
        return self._build_mode_policy(
            mode_key="fortress",
            policy_name="strict",
            expected_move=expected_move,
            spot=spot,
            target_em_multiple=fortress_em_multiple,
            percent_floor=None,
            min_credit=None,
            min_risk_efficiency=None,
            pass_type="fortress",
            pass_type_label="Fortress",
            fallback_used=False,
        )

    def _build_fortress_candidate(
        self,
        *,
        mode: Dict[str, Any],
        policy: Dict[str, Any],
        spread: Dict[str, Any],
        contracts: int,
        selected_variant: str,
        selection_note: str,
    ) -> Dict[str, Any]:
        credit = max(float(spread.get("executable_credit") or 0.0), 0.0)
        width = float(spread.get("width") or 0.0)
        premium_per_contract = credit * 100
        total_premium = premium_per_contract * contracts
        max_loss_per_contract_dollars = max((width - credit) * 100, 0.0)
        total_max_loss = max_loss_per_contract_dollars * contracts
        credit_efficiency_pct = self._calculate_credit_efficiency_pct(
            total_premium=total_premium,
            total_max_loss=total_max_loss,
        )
        risk_efficiency = (credit_efficiency_pct / 100.0) if credit_efficiency_pct is not None else None
        short_put = spread["short_put"]
        long_put = spread["long_put"]
        short_delta = spread.get("short_delta")
        em_multiple = float(spread.get("em_multiple") or 0.0)
        break_even = float(spread.get("break_even") or 0.0)
        em_buffer = (float(spread.get("spot") or 0.0) - float(spread.get("expected_move") or 0.0)) - break_even
        realistic_max_loss_dollars = self._estimate_realistic_max_loss(total_max_loss)
        diagnostics = [
            selection_note,
            "Fortress pricing basis: Short = Bid, Long = Ask.",
            f"Base EM rule used: {policy.get('boundary_rule_used') or 'Fortress EM rule'}.",
            f"Net credit / contract: ${credit:,.2f} using short bid {float(short_put.get('bid') or 0.0):,.2f} and long ask {float(long_put.get('ask') or 0.0):,.2f}.",
            f"Total premium received: ${total_premium:,.0f} across {contracts} contract{'s' if contracts != 1 else ''}.",
            f"Max theoretical loss: ${total_max_loss:,.0f} total | ${max_loss_per_contract_dollars:,.0f} per contract.",
            f"Credit efficiency: {(credit_efficiency_pct or 0.0):.1f}% = premium received / max theoretical loss.",
            f"Break-even buffer vs lower expected move boundary: {em_buffer:,.2f} points.",
        ]
        rationale = [
            f"Fortress must clear {policy.get('target_em_multiple', 0.0):.2f}x expected move before any optimization is considered.",
            f"The selected short strike is {float(short_put['strike']):,.0f}, {float(spread['distance_points']):,.2f} points from spot at {em_multiple:,.2f}x expected move.",
            "Selection priority is credit efficiency first, then highest total premium, then lowest max loss.",
        ]
        return {
            "strategy_label": f"Sell {int(short_put['strike'])} / Buy {int(long_put['strike'])} put credit spread",
            "short_strike": short_put["strike"],
            "long_strike": long_put["strike"],
            "width": width,
            "credit": round(credit, 2),
            "net_credit_per_contract": round(credit, 2),
            "premium_per_contract": round(premium_per_contract, 2),
            "premium_received_dollars": round(total_premium, 2),
            "total_premium": round(total_premium, 2),
            "total_premium_received": round(total_premium, 2),
            "max_theoretical_risk": round(total_max_loss, 2),
            "max_theoretical_risk_per_contract": round(max_loss_per_contract_dollars, 2),
            "total_max_theoretical_loss": round(total_max_loss, 2),
            "risk_efficiency": round(risk_efficiency, 4) if risk_efficiency is not None else None,
            "credit_efficiency_pct": round(credit_efficiency_pct, 2) if credit_efficiency_pct is not None else None,
            "max_loss": round(total_max_loss, 2),
            "max_loss_per_contract": round(width - credit, 2),
            "break_even": round(break_even, 2),
            "short_delta": round(short_delta, 2) if short_delta is not None else None,
            "long_delta": round(long_put.get("delta_abs"), 2) if long_put.get("delta_abs") is not None else None,
            "distance_points": round(float(spread.get("distance_points") or 0.0), 2),
            "distance_percent": round(float(spread.get("distance_percent") or 0.0) * 100, 2),
            "expected_move": round(float(spread.get("expected_move") or 0.0), 2),
            "expected_move_used": round(float(spread.get("expected_move") or 0.0), 2),
            "expected_move_source": "atm_straddle_estimate",
            "expected_move_1_5x_threshold": round(float(spread.get("expected_move") or 0.0) * 1.5, 2),
            "expected_move_2x_threshold": round(float(spread.get("expected_move") or 0.0) * 2.0, 2),
            "expected_move_comparison": self._build_expected_move_comparison(policy=policy, spread=spread),
            "required_distance_rule_used": policy.get("distance_threshold_label") or "—",
            "active_em_rule": policy.get("active_rule_set") or "—",
            "active_rule_set": policy.get("active_rule_set") or "—",
            "pass_type": policy.get("pass_type") or "fortress",
            "pass_type_label": policy.get("pass_type_label") or "Fortress",
            "target_em_multiple": round(float(policy.get("target_em_multiple") or 0.0), 2),
            "target_em": round(float(policy.get("target_em_multiple") or 0.0), 2),
            "applied_em_multiple_floor": round(float(policy.get("applied_em_multiple_floor") or 0.0), 2),
            "percent_floor": None,
            "percent_floor_points": None,
            "em_floor_points": round(float(policy.get("em_floor_points") or 0.0), 2),
            "hybrid_distance_threshold": round(float(policy.get("distance_threshold") or 0.0), 2),
            "boundary_binding_source": policy.get("boundary_binding_source") or "EM Floor",
            "boundary_rule_used": policy.get("boundary_rule_used") or "—",
            "recommended_contract_size": contracts,
            "fallback_used": False,
            "fallback_rule_name": "",
            "recommended_contract_size_reason": (
                "Fortress base uses 10 contracts."
                if selected_variant == "base"
                else "Fortress optimizer selected this contract size using the credit-efficiency priority stack."
            ),
            "score": int(round((credit_efficiency_pct or 0.0) * 10)),
            "rationale": rationale,
            "exit_plan": self._build_exit_plan(short_put["strike"], long_put["strike"]),
            "diagnostics": diagnostics,
            "short_open_interest": round(float(spread.get("short_open_interest") or 0.0)),
            "short_volume": round(float(spread.get("short_volume") or 0.0)),
            "short_bid": round(float(short_put.get("bid") or 0.0), 2),
            "short_ask": round(float(short_put.get("ask") or 0.0), 2),
            "long_bid": round(float(long_put.get("bid") or 0.0), 2),
            "long_ask": round(float(long_put.get("ask") or 0.0), 2),
            "pricing_basis": "Short = Bid, Long = Ask",
            "executable_credit": round(credit, 2),
            "mid_credit": round(float(spread.get("mid_credit") or 0.0), 2),
            "hybrid_credit": round(float(spread.get("hybrid_credit") or 0.0), 2),
            "premium_ratio_to_standard": None,
            "max_loss_ratio_to_standard": None,
            "meets_premium_target_band": True,
            "meets_black_swan_risk_target": total_max_loss <= self.FORTRESS_MAX_LOSS_CAP_DOLLARS,
            "qualifies_for_full_size": contracts == self.FORTRESS_BASE_CONTRACTS,
            "distance_above_threshold": round(float(spread.get("distance_points") or 0.0) - float(policy.get("distance_threshold") or 0.0), 2),
            "em_buffer": round(em_buffer, 2),
            "em_multiple": round(em_multiple, 2),
            "actual_distance_to_short": round(float(spread.get("distance_points") or 0.0), 2),
            "actual_em_multiple": round(em_multiple, 2),
            "risk_cap_dollars": self.FORTRESS_MAX_LOSS_CAP_DOLLARS,
            "risk_cap_status": "Within $15,000 max loss cap" if total_max_loss <= self.FORTRESS_MAX_LOSS_CAP_DOLLARS else "Base fallback used above optimization cap",
            "risk_cap_adjusted": False,
            "original_contract_size": contracts,
            "adjusted_contract_size": contracts,
            "account_risk_percent": round((total_max_loss / self.account_value) * 100, 2) if self.account_value else None,
            "realistic_max_loss": round(realistic_max_loss_dollars, 2),
            "exit_plan_applied": True,
            "available": True,
            "no_trade_message": "",
            "selection_variant": selected_variant,
        }

    def _build_fortress_evaluation_log(
        self,
        *,
        mode: Dict[str, Any],
        policy: Dict[str, Any],
        candidate: Dict[str, Any],
        reject_reason: str,
    ) -> Dict[str, Any]:
        return {
            "mode_label": mode["label"],
            "short_strike": round(float(candidate.get("short_strike") or 0.0), 2),
            "long_strike": round(float(candidate.get("long_strike") or 0.0), 2),
            "distance_points": round(float(candidate.get("distance_points") or 0.0), 2),
            "distance_percent": round(float(candidate.get("distance_percent") or 0.0), 2),
            "expected_move": round(float(candidate.get("expected_move") or 0.0), 2),
            "expected_move_1_5x_threshold": round(float(candidate.get("expected_move_1_5x_threshold") or 0.0), 2),
            "two_x_expected_move_threshold": round(float(candidate.get("expected_move_2x_threshold") or 0.0), 2),
            "required_distance_mode": policy.get("distance_threshold_label") or "—",
            "active_rule_set": policy.get("active_rule_set") or "—",
            "pass_type": policy.get("pass_type") or "fortress",
            "pass_type_label": policy.get("pass_type_label") or "Fortress",
            "fallback_used": False,
            "fallback_rule_name": "",
            "target_em_multiple": round(float(policy.get("target_em_multiple") or 0.0), 2),
            "percent_floor": None,
            "minimum_credit_per_contract": 0.0,
            "minimum_risk_efficiency": None,
            "percent_floor_points": None,
            "em_floor_points": round(float(policy.get("em_floor_points") or 0.0), 2),
            "hybrid_distance_threshold": round(float(policy.get("distance_threshold") or 0.0), 2),
            "boundary_binding_source": policy.get("boundary_binding_source") or "EM Floor",
            "boundary_rule_used": policy.get("boundary_rule_used") or "—",
            "macro_modifier_status": "Yes" if mode.get("macro_present") else "No",
            "structure_modifier_status": "Yes" if mode.get("structure_grade") == "Poor" else "No",
            "pricing_basis": "Short = Bid, Long = Ask",
            "executable_credit": round(float(candidate.get("credit") or 0.0), 2),
            "mid_credit": round(float(candidate.get("mid_credit") or 0.0), 2),
            "hybrid_credit": round(float(candidate.get("hybrid_credit") or 0.0), 2),
            "net_credit": round(float(candidate.get("credit") or 0.0), 2),
            "premium_per_contract": round(float(candidate.get("premium_per_contract") or 0.0), 2),
            "premium_received_dollars": round(float(candidate.get("premium_received_dollars") or 0.0), 2),
            "max_theoretical_risk": round(float(candidate.get("max_loss") or 0.0), 2),
            "max_theoretical_risk_per_contract": round(float(candidate.get("max_theoretical_risk_per_contract") or 0.0), 2),
            "risk_efficiency": round(float(candidate.get("risk_efficiency") or 0.0), 4) if candidate.get("risk_efficiency") is not None else None,
            "max_loss_dollars": round(float(candidate.get("max_loss") or 0.0), 2),
            "short_delta": round(float(candidate.get("short_delta") or 0.0), 3) if candidate.get("short_delta") is not None else None,
            "contract_size_chosen": int(candidate.get("recommended_contract_size") or 0),
            "qualifies_for_full_size": "Yes" if int(candidate.get("recommended_contract_size") or 0) == self.FORTRESS_BASE_CONTRACTS else "No",
            "risk_cap_dollars": self.FORTRESS_MAX_LOSS_CAP_DOLLARS,
            "risk_cap_status": candidate.get("risk_cap_status") or "—",
            "original_contract_size": int(candidate.get("original_contract_size") or 0),
            "account_risk_percent": round(float(candidate.get("account_risk_percent") or 0.0), 2) if candidate.get("account_risk_percent") is not None else None,
            "premium_ratio_to_standard": None,
            "max_loss_ratio_to_standard": None,
            "reject_reason": reject_reason,
        }

    @staticmethod
    def _build_fortress_candidate_summary(candidate: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "short_strike": round(float(candidate.get("short_strike") or 0.0), 2),
            "long_strike": round(float(candidate.get("long_strike") or 0.0), 2),
            "width": round(float(candidate.get("width") or 0.0), 2),
            "contracts": int(candidate.get("recommended_contract_size") or 0),
            "net_credit_per_contract": round(float(candidate.get("credit") or 0.0), 2),
            "total_premium_received": round(float(candidate.get("total_premium") or 0.0), 2),
            "total_max_theoretical_loss": round(float(candidate.get("max_loss") or 0.0), 2),
            "credit_efficiency_pct": round(float(candidate.get("credit_efficiency_pct") or 0.0), 2),
            "em_multiple": round(float(candidate.get("em_multiple") or 0.0), 2),
        }

    @staticmethod
    def _fortress_selection_priority_key(item: Dict[str, Any]) -> tuple:
        return (
            float(item.get("credit_efficiency_pct") or 0.0),
            float(item.get("total_premium") or item.get("premium_received_dollars") or 0.0),
            -float(item.get("max_loss") or 0.0),
        )

    @staticmethod
    def _calculate_credit_efficiency_pct(*, total_premium: float, total_max_loss: float) -> float | None:
        if total_max_loss <= 0:
            return None
        return (total_premium / total_max_loss) * 100.0

    @staticmethod
    def _resolve_fortress_em_multiple(*, standard_em_multiple: float, structure_grade: str, macro_grade: str) -> float:
        fortress_em = max(float(standard_em_multiple or 0.0), 0.0) * 1.10
        structure_poor = str(structure_grade or "").strip().title() == "Poor"
        macro_major = str(macro_grade or "").strip().title() == "Major"
        if macro_major:
            fortress_em += 0.25
        if structure_poor:
            fortress_em += 0.25
        if macro_major and structure_poor:
            fortress_em += 0.50
        return max(fortress_em, float(standard_em_multiple or 0.0))

    def _order_spread_pool_for_mode(self, mode: Dict[str, Any], spread_pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if mode["key"] != "fortress":
            return list(spread_pool)
        return sorted(
            spread_pool,
            key=lambda spread: (
                float(spread.get("width") or 0.0),
                -float(spread.get("distance_points") or 0.0),
                -max(float(spread.get("executable_credit") or 0.0), 0.0),
                float(spread.get("short_delta") or 99.0),
            ),
        )

    @staticmethod
    def _fortress_candidate_sort_key(item: Dict[str, Any]) -> tuple:
        return (
            float(item.get("premium_received_dollars") or 0.0),
            float(item.get("risk_efficiency") or 0.0),
            float(item.get("distance_points") or 0.0),
            -(float(item.get("short_delta") or 0.0)),
            float(item.get("em_multiple") or 0.0),
        )

    def _evaluate_mode_candidate(
        self,
        mode: Dict[str, Any],
        policy: Dict[str, Any],
        spread: Dict[str, Any],
        spread_pool: List[Dict[str, Any]],
        structure: Dict[str, Any],
        macro: Dict[str, Any],
        standard_reference: Dict[str, Any] | None = None,
    ) -> tuple[Dict[str, Any] | None, str | None, Dict[str, Any]]:
        short_put = spread["short_put"]
        long_put = spread["long_put"]
        distance_points = spread["distance_points"]
        distance_percent = spread["distance_percent"]
        expected_move = spread["expected_move"]
        short_delta = spread["short_delta"]
        pricing_context = self._resolve_mode_pricing_context(mode, spread)
        credit = pricing_context["selected_credit"]
        width = spread["width"]
        em_multiple = spread["em_multiple"]

        distance_pass = distance_points >= policy["distance_threshold"]
        delta_preferred = short_delta is not None and short_delta <= mode["delta_preferred"]
        delta_pass = short_delta is not None and short_delta <= mode["delta_max"]
        contract_size, contract_size_reason = self._derive_contract_size(mode=mode, spread=spread)
        premium_per_contract = credit * 100
        max_theoretical_risk_per_contract = max((width - credit) * 100, 0.0)
        risk_efficiency = self._calculate_risk_efficiency(
            premium_collected=premium_per_contract,
            maximum_theoretical_risk=max_theoretical_risk_per_contract,
        )
        if mode["key"] == "fortress":
            risk_overlay = self._resolve_fortress_targeting(
                spread=spread,
                credit=credit,
                baseline_contract_size=contract_size,
                standard_reference=standard_reference,
            )
        else:
            risk_overlay = self._apply_risk_overlay(
                mode=mode,
                width=width,
                credit=credit,
                original_contract_size=contract_size,
                base_reason=contract_size_reason,
            )
        contract_size = risk_overlay["adjusted_contract_size"]
        contract_size_reason = risk_overlay["contract_size_reason"]
        premium_received_dollars = risk_overlay["premium_received_dollars"]
        max_loss_dollars = risk_overlay["max_loss_dollars"]
        position_risk_efficiency = self._calculate_risk_efficiency(
            premium_collected=premium_received_dollars,
            maximum_theoretical_risk=max_loss_dollars,
        )
        position_credit_efficiency_pct = self._calculate_credit_efficiency_pct(
            total_premium=premium_received_dollars,
            total_max_loss=max_loss_dollars,
        )
        break_even = spread["break_even"]
        lower_expected_move = spread["spot"] - expected_move
        em_buffer = lower_expected_move - break_even
        realistic_max_loss_dollars = self._estimate_realistic_max_loss(max_loss_dollars)

        log = {
            "mode_label": mode["label"],
            "short_strike": round(short_put["strike"], 2),
            "long_strike": round(long_put["strike"], 2),
            "distance_points": round(distance_points, 2),
            "distance_percent": round(distance_percent * 100, 2),
            "expected_move": round(expected_move, 2),
            "expected_move_1_5x_threshold": round(expected_move * 1.5, 2),
            "two_x_expected_move_threshold": round(expected_move * 2.0, 2),
            "required_distance_mode": policy["distance_threshold_label"],
            "active_rule_set": policy["active_rule_set"],
            "pass_type": policy["pass_type"],
            "pass_type_label": policy.get("pass_type_label") or policy["pass_type"],
            "fallback_used": policy["fallback_used"],
            "fallback_rule_name": policy["fallback_rule_name"],
            "target_em_multiple": round(policy["target_em_multiple"], 2),
            "percent_floor": round(policy["percent_floor"], 2) if policy.get("percent_floor") is not None else None,
            "minimum_credit_per_contract": round(float(policy.get("min_credit") or 0.0), 2),
            "minimum_risk_efficiency": round(float(policy.get("min_risk_efficiency") or 0.0), 4) if policy.get("min_risk_efficiency") is not None else None,
            "percent_floor_points": round(policy["percent_floor_points"], 2) if policy.get("percent_floor_points") is not None else None,
            "em_floor_points": round(policy["em_floor_points"], 2),
            "hybrid_distance_threshold": round(policy["distance_threshold"], 2),
            "boundary_binding_source": policy["boundary_binding_source"],
            "boundary_rule_used": policy["boundary_rule_used"],
            "macro_modifier_status": "Yes" if mode.get("macro_present") else "No",
            "structure_modifier_status": "Yes" if mode.get("structure_grade") == "Poor" else "No",
            "pricing_basis": pricing_context["pricing_basis"],
            "executable_credit": round(pricing_context["executable_credit"], 2),
            "mid_credit": round(pricing_context["mid_credit"], 2),
            "hybrid_credit": round(pricing_context["hybrid_credit"], 2),
            "net_credit": round(credit, 2),
            "premium_per_contract": round(premium_per_contract, 2),
            "premium_received_dollars": round(premium_received_dollars, 2),
            "max_theoretical_risk": round(max_loss_dollars, 2),
            "max_theoretical_risk_per_contract": round(max_theoretical_risk_per_contract, 2),
            "risk_efficiency": round(position_risk_efficiency, 4) if position_risk_efficiency is not None else None,
            "credit_efficiency_pct": round(position_credit_efficiency_pct, 2) if position_credit_efficiency_pct is not None else None,
            "max_loss_dollars": round(max_loss_dollars, 2),
            "short_delta": round(short_delta, 3) if short_delta is not None else None,
            "contract_size_chosen": contract_size,
            "qualifies_for_full_size": "Yes" if contract_size >= 8 else "No",
            "risk_cap_dollars": round(risk_overlay["risk_cap_dollars"], 2),
            "risk_cap_status": risk_overlay["risk_cap_status"],
            "original_contract_size": risk_overlay["original_contract_size"],
            "account_risk_percent": round(risk_overlay["account_risk_percent"], 2) if risk_overlay["account_risk_percent"] is not None else None,
            "premium_ratio_to_standard": round(float(risk_overlay["premium_ratio_to_standard"]), 4) if risk_overlay.get("premium_ratio_to_standard") is not None else None,
            "max_loss_ratio_to_standard": round(float(risk_overlay["max_loss_ratio_to_standard"]), 4) if risk_overlay.get("max_loss_ratio_to_standard") is not None else None,
            "reject_reason": None,
        }

        if spread["short_market_width"] is not None and spread["short_market_width"] > self.MAX_SHORT_BID_ASK_WIDTH:
            log["reject_reason"] = "Short-leg bid/ask spread too wide"
            return None, log["reject_reason"], log
        if not distance_pass:
            log["reject_reason"] = f"Failed {policy['boundary_rule_used']} distance rule"
            return None, log["reject_reason"], log
        if not delta_pass:
            log["reject_reason"] = f"Short delta above {mode['delta_max']:.2f}"
            return None, log["reject_reason"], log
        if policy.get("min_risk_efficiency") is not None:
            required_risk_efficiency = float(policy["min_risk_efficiency"])
            if risk_efficiency is None or risk_efficiency < required_risk_efficiency:
                log["reject_reason"] = f"Risk efficiency below {required_risk_efficiency:.2f}"
                return None, log["reject_reason"], log
        if mode["key"] != "fortress":
            required_credit = float(policy.get("min_credit") or self.MIN_NET_CREDIT)
            if credit < required_credit:
                log["reject_reason"] = f"Net credit below {required_credit:.2f}"
                return None, log["reject_reason"], log
        if mode["key"] != "fortress" and spread["credit_to_width"] < self.MIN_CREDIT_TO_WIDTH:
            log["reject_reason"] = "Credit-to-width ratio too small"
            return None, log["reject_reason"], log
        if spread["short_volume"] <= 0 and spread["short_open_interest"] <= 0:
            log["reject_reason"] = "Short leg lacks liquidity"
            return None, log["reject_reason"], log

        if not risk_overlay["valid"]:
            log["reject_reason"] = str(risk_overlay.get("risk_cap_status") or "Rejected by risk overlay")
            candidate = self._build_invalid_candidate(
                mode=mode,
                policy=policy,
                spread=spread,
                pricing_context=pricing_context,
                risk_overlay=risk_overlay,
                log=log,
            )
            return candidate, log["reject_reason"], log

        rationale = self._build_rationale(mode=mode, spread=spread, contract_size=contract_size, contract_size_reason=contract_size_reason)
        diagnostics = self._build_diagnostics(
            mode=mode,
            policy=policy,
            spread=spread,
            pricing_context=pricing_context,
            contract_size=contract_size,
            contract_size_reason=contract_size_reason,
            premium_received_dollars=premium_received_dollars,
            max_loss_dollars=max_loss_dollars,
            distance_pass=distance_pass,
            delta_preferred=delta_preferred,
            delta_pass=delta_pass,
            em_buffer=em_buffer,
            risk_overlay=risk_overlay,
            realistic_max_loss_dollars=realistic_max_loss_dollars,
        )

        log["reject_reason"] = "Passed"
        return {
            "strategy_label": f"Sell {int(short_put['strike'])} / Buy {int(long_put['strike'])} put credit spread",
            "short_strike": short_put["strike"],
            "long_strike": long_put["strike"],
            "width": width,
            "credit": round(credit, 2),
            "premium_per_contract": round(premium_per_contract, 2),
            "premium_received_dollars": round(premium_received_dollars, 2),
            "total_premium": round(premium_received_dollars, 2),
            "max_theoretical_risk": round(max_loss_dollars, 2),
            "max_theoretical_risk_per_contract": round(max_theoretical_risk_per_contract, 2),
            "risk_efficiency": round(position_risk_efficiency, 4) if position_risk_efficiency is not None else None,
            "credit_efficiency_pct": round(position_credit_efficiency_pct, 2) if position_credit_efficiency_pct is not None else None,
            "max_loss": round(max_loss_dollars, 2),
            "max_loss_per_contract": round(width - credit, 2),
            "break_even": round(break_even, 2),
            "short_delta": round(short_delta, 2) if short_delta is not None else None,
            "long_delta": round(long_put.get("delta_abs"), 2) if long_put.get("delta_abs") is not None else None,
            "distance_points": round(distance_points, 2),
            "distance_percent": round(distance_percent * 100, 2),
            "expected_move": round(expected_move, 2),
            "expected_move_used": round(expected_move, 2),
            "expected_move_source": "atm_straddle_estimate",
            "expected_move_1_5x_threshold": round(expected_move * 1.5, 2),
            "expected_move_2x_threshold": round(expected_move * 2.0, 2),
            "expected_move_comparison": self._build_expected_move_comparison(policy=policy, spread=spread),
            "required_distance_rule_used": policy["distance_threshold_label"],
            "active_em_rule": policy["active_rule_set"],
            "active_rule_set": policy["active_rule_set"],
            "pass_type_label": policy.get("pass_type_label") or policy["pass_type"],
            "target_em_multiple": round(policy["target_em_multiple"], 2),
            "target_em": round(policy["target_em_multiple"], 2),
            "applied_em_multiple_floor": round(policy["applied_em_multiple_floor"], 2),
            "percent_floor": round(policy["percent_floor"], 2) if policy.get("percent_floor") is not None else None,
            "percent_floor_points": round(policy["percent_floor_points"], 2) if policy.get("percent_floor_points") is not None else None,
            "em_floor_points": round(policy["em_floor_points"], 2),
            "hybrid_distance_threshold": round(policy["distance_threshold"], 2),
            "boundary_binding_source": policy["boundary_binding_source"],
            "boundary_rule_used": policy["boundary_rule_used"],
            "pass_type": policy["pass_type"],
            "fallback_used": policy["fallback_used"],
            "fallback_rule_name": policy["fallback_rule_name"],
            "recommended_contract_size": contract_size,
            "recommended_contract_size_reason": contract_size_reason,
            "score": self._build_mode_score(mode=mode, spread=spread, delta_preferred=delta_preferred, contract_size=contract_size),
            "rationale": rationale,
            "exit_plan": self._build_exit_plan(short_put["strike"], long_put["strike"]),
            "diagnostics": diagnostics,
            "short_open_interest": round(spread["short_open_interest"]),
            "short_volume": round(spread["short_volume"]),
            "short_bid": round(short_put["bid"], 2),
            "short_ask": round(short_put["ask"], 2),
            "long_bid": round(long_put["bid"], 2),
            "long_ask": round(long_put["ask"], 2),
            "pricing_basis": pricing_context["pricing_basis"],
            "executable_credit": round(pricing_context["executable_credit"], 2),
            "mid_credit": round(pricing_context["mid_credit"], 2),
            "hybrid_credit": round(pricing_context["hybrid_credit"], 2),
            "premium_ratio_to_standard": round(float(risk_overlay["premium_ratio_to_standard"]), 4) if risk_overlay.get("premium_ratio_to_standard") is not None else None,
            "max_loss_ratio_to_standard": round(float(risk_overlay["max_loss_ratio_to_standard"]), 4) if risk_overlay.get("max_loss_ratio_to_standard") is not None else None,
            "meets_premium_target_band": bool(risk_overlay.get("meets_premium_target_band")),
            "meets_black_swan_risk_target": bool(risk_overlay.get("meets_black_swan_risk_target")),
            "qualifies_for_full_size": contract_size >= 8,
            "distance_above_threshold": round(distance_points - policy["distance_threshold"], 2),
            "em_buffer": round(em_buffer, 2),
            "em_multiple": round(em_multiple, 2),
            "actual_distance_to_short": round(distance_points, 2),
            "actual_em_multiple": round(em_multiple, 2),
            "risk_cap_dollars": round(risk_overlay["risk_cap_dollars"], 2),
            "risk_cap_status": risk_overlay["risk_cap_status"],
            "risk_cap_adjusted": risk_overlay["adjusted_for_cap"],
            "original_contract_size": risk_overlay["original_contract_size"],
            "adjusted_contract_size": risk_overlay["adjusted_contract_size"],
            "account_risk_percent": round(risk_overlay["account_risk_percent"], 2) if risk_overlay["account_risk_percent"] is not None else None,
            "realistic_max_loss": round(realistic_max_loss_dollars, 2),
            "exit_plan_applied": True,
            "available": True,
            "no_trade_message": "",
        }, None, log

    def _build_spread_snapshot(
        self,
        short_put: Dict[str, Any],
        long_put: Dict[str, Any],
        width: float,
        spot: float,
        expected_move: float,
    ) -> Dict[str, Any]:
        short_bid = short_put.get("bid") or 0.0
        short_ask = short_put.get("ask") or 0.0
        long_ask = long_put.get("ask") or 0.0
        conservative_credit = short_bid - long_ask
        mid_credit = self._premium_anchor(short_put) - self._premium_anchor(long_put)
        hybrid_credit = max(conservative_credit, mid_credit)
        distance_points = spot - short_put["strike"]
        short_delta = short_put.get("delta_abs")
        long_delta = long_put.get("delta_abs")
        return {
            "short_put": short_put,
            "long_put": long_put,
            "width": width,
            "spot": spot,
            "expected_move": expected_move,
            "distance_points": distance_points,
            "distance_percent": (distance_points / spot) if spot > 0 else 0.0,
            "em_multiple": (distance_points / expected_move) if expected_move else 0.0,
            "short_delta": short_delta,
            "long_delta": long_delta,
            "credit": hybrid_credit,
            "executable_credit": conservative_credit,
            "mid_credit": mid_credit,
            "hybrid_credit": hybrid_credit,
            "credit_to_width": (hybrid_credit / width) if width > 0 else 0.0,
            "break_even": short_put["strike"] - hybrid_credit,
            "short_market_width": (short_ask - short_bid) if short_ask and short_bid else None,
            "short_open_interest": short_put.get("open_interest") or 0.0,
            "short_volume": short_put.get("total_volume") or 0.0,
        }

    def _resolve_mode_pricing_context(self, mode: Dict[str, Any], spread: Dict[str, Any]) -> Dict[str, Any]:
        executable_credit = max(float(spread.get("executable_credit") or 0.0), 0.0)
        mid_credit = max(float(spread.get("mid_credit") or 0.0), 0.0)
        hybrid_credit = max(float(spread.get("hybrid_credit") or 0.0), executable_credit, mid_credit)
        if mode["key"] == "fortress":
            return {
                "pricing_basis": "Executable short bid - long ask",
                "selected_credit": executable_credit,
                "executable_credit": executable_credit,
                "mid_credit": mid_credit,
                "hybrid_credit": hybrid_credit,
            }
        return {
            "pricing_basis": "Hybrid max(executable, midpoint)",
            "selected_credit": hybrid_credit,
            "executable_credit": executable_credit,
            "mid_credit": mid_credit,
            "hybrid_credit": hybrid_credit,
        }

    def _resolve_fortress_targeting(
        self,
        *,
        spread: Dict[str, Any],
        credit: float,
        baseline_contract_size: int,
        standard_reference: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        global_risk_cap_dollars = self._risk_cap_dollars("fortress")
        per_contract_max_loss = max((spread["width"] - credit) * 100, 0.0)
        premium_per_contract = max(credit * 100, 0.0)
        if standard_reference is None:
            return {
                "valid": False,
                "adjusted_for_cap": False,
                "original_contract_size": baseline_contract_size,
                "adjusted_contract_size": 0,
                "contract_size_reason": "Fortress requires a valid Standard reference before it can target premium and risk bands.",
                "risk_cap_dollars": global_risk_cap_dollars,
                "risk_cap_status": "Rejected: Standard reference unavailable",
                "premium_received_dollars": 0.0,
                "max_loss_dollars": per_contract_max_loss,
                "realistic_max_loss_dollars": self._estimate_realistic_max_loss(per_contract_max_loss),
                "account_risk_percent": (per_contract_max_loss / self.account_value) * 100 if self.account_value else None,
                "premium_ratio_to_standard": None,
                "max_loss_ratio_to_standard": None,
                "meets_premium_target_band": False,
                "meets_black_swan_risk_target": False,
            }

        standard_total_premium = float(standard_reference.get("total_premium") or standard_reference.get("premium_received_dollars") or 0.0)
        standard_max_loss = float(standard_reference.get("max_theoretical_risk") or standard_reference.get("max_loss") or 0.0)
        if standard_total_premium <= 0 or standard_max_loss <= 0 or per_contract_max_loss <= 0 or premium_per_contract <= 0:
            return {
                "valid": False,
                "adjusted_for_cap": False,
                "original_contract_size": baseline_contract_size,
                "adjusted_contract_size": 0,
                "contract_size_reason": "Fortress could not derive a positive executable premium and max-loss path relative to the selected Standard trade.",
                "risk_cap_dollars": global_risk_cap_dollars,
                "risk_cap_status": "Rejected: invalid Standard-relative reference",
                "premium_received_dollars": 0.0,
                "max_loss_dollars": per_contract_max_loss,
                "realistic_max_loss_dollars": self._estimate_realistic_max_loss(per_contract_max_loss),
                "account_risk_percent": (per_contract_max_loss / self.account_value) * 100 if self.account_value else None,
                "premium_ratio_to_standard": None,
                "max_loss_ratio_to_standard": None,
                "meets_premium_target_band": False,
                "meets_black_swan_risk_target": False,
            }

        premium_target_min = standard_total_premium * self.FORTRESS_TARGET_PREMIUM_RATIO_MIN
        premium_target_max = standard_total_premium * self.FORTRESS_TARGET_PREMIUM_RATIO_MAX
        premium_target_ideal = standard_total_premium * self.FORTRESS_TARGET_PREMIUM_RATIO_IDEAL
        standard_risk_target_dollars = standard_max_loss * self.FORTRESS_MAX_STANDARD_RISK_RATIO
        effective_risk_cap = min(global_risk_cap_dollars, standard_risk_target_dollars)
        max_contracts = int(effective_risk_cap // per_contract_max_loss) if per_contract_max_loss > 0 else 0
        if max_contracts < 1:
            cap_status = (
                "Rejected: exceeds max loss cap"
                if effective_risk_cap <= global_risk_cap_dollars + 0.01
                else "Rejected: Standard-relative risk cap"
            )
            cap_reason = (
                "Fortress could not fit even one contract inside the total max-loss cap."
                if cap_status == "Rejected: exceeds max loss cap"
                else "Fortress could not fit even one contract inside the Standard-relative 50% risk cap."
            )
            return {
                "valid": False,
                "adjusted_for_cap": False,
                "original_contract_size": baseline_contract_size,
                "adjusted_contract_size": 0,
                "contract_size_reason": cap_reason,
                "risk_cap_dollars": effective_risk_cap,
                "risk_cap_status": cap_status,
                "premium_received_dollars": 0.0,
                "max_loss_dollars": per_contract_max_loss,
                "realistic_max_loss_dollars": self._estimate_realistic_max_loss(per_contract_max_loss),
                "account_risk_percent": (per_contract_max_loss / self.account_value) * 100 if self.account_value else None,
                "premium_ratio_to_standard": None,
                "max_loss_ratio_to_standard": None,
                "meets_premium_target_band": False,
                "meets_black_swan_risk_target": False,
            }

        viable_sizes = []
        for size in range(1, max_contracts + 1):
            total_premium = premium_per_contract * size
            max_loss_dollars = per_contract_max_loss * size
            premium_ratio = total_premium / standard_total_premium if standard_total_premium > 0 else None
            max_loss_ratio = max_loss_dollars / standard_max_loss if standard_max_loss > 0 else None
            meets_premium_target_band = premium_target_min <= total_premium <= premium_target_max
            meets_black_swan_risk_target = max_loss_dollars <= standard_risk_target_dollars + 0.01
            if not meets_black_swan_risk_target:
                continue
            viable_sizes.append(
                {
                    "contract_size": size,
                    "total_premium": total_premium,
                    "max_loss_dollars": max_loss_dollars,
                    "premium_ratio_to_standard": premium_ratio,
                    "max_loss_ratio_to_standard": max_loss_ratio,
                    "meets_premium_target_band": meets_premium_target_band,
                    "meets_black_swan_risk_target": meets_black_swan_risk_target,
                    "premium_gap": abs(total_premium - premium_target_ideal),
                }
            )

        preferred_sizes = [item for item in viable_sizes if item["meets_premium_target_band"]]
        if not preferred_sizes:
            return {
                "valid": False,
                "adjusted_for_cap": False,
                "original_contract_size": baseline_contract_size,
                "adjusted_contract_size": 0,
                "contract_size_reason": "Fortress stayed inside the risk cap but could not reach the 40-60% Standard premium band using executable pricing.",
                "risk_cap_dollars": effective_risk_cap,
                "risk_cap_status": "Rejected: premium target band not met",
                "premium_received_dollars": 0.0,
                "max_loss_dollars": per_contract_max_loss,
                "realistic_max_loss_dollars": self._estimate_realistic_max_loss(per_contract_max_loss),
                "account_risk_percent": (per_contract_max_loss / self.account_value) * 100 if self.account_value else None,
                "premium_ratio_to_standard": None,
                "max_loss_ratio_to_standard": None,
                "meets_premium_target_band": False,
                "meets_black_swan_risk_target": False,
            }

        selected = min(
            preferred_sizes,
            key=lambda item: (
                item["premium_gap"],
                -item["total_premium"],
                item["max_loss_ratio_to_standard"] or 0.0,
            ),
        )
        contract_size = int(selected["contract_size"])
        premium_ratio = float(selected["premium_ratio_to_standard"] or 0.0)
        max_loss_ratio = float(selected["max_loss_ratio_to_standard"] or 0.0)
        return {
            "valid": True,
            "adjusted_for_cap": contract_size != baseline_contract_size,
            "original_contract_size": baseline_contract_size,
            "adjusted_contract_size": contract_size,
            "contract_size_reason": f"Fortress sizes to {contract_size} contracts so executable premium lands at {premium_ratio * 100:.0f}% of Standard while max loss stays at {max_loss_ratio * 100:.0f}% of Standard.",
            "risk_cap_dollars": effective_risk_cap,
            "risk_cap_status": "Within Standard-relative Fortress risk cap",
            "premium_received_dollars": round(selected["total_premium"], 2),
            "max_loss_dollars": round(selected["max_loss_dollars"], 2),
            "realistic_max_loss_dollars": self._estimate_realistic_max_loss(selected["max_loss_dollars"]),
            "account_risk_percent": (selected["max_loss_dollars"] / self.account_value) * 100 if self.account_value else None,
            "premium_ratio_to_standard": premium_ratio,
            "max_loss_ratio_to_standard": max_loss_ratio,
            "meets_premium_target_band": True,
            "meets_black_swan_risk_target": True,
            "standard_total_premium": round(standard_total_premium, 2),
            "standard_max_loss": round(standard_max_loss, 2),
        }

    def _derive_contract_size(self, mode: Dict[str, Any], spread: Dict[str, Any]) -> tuple[int, str]:
        if mode["key"] == "standard":
            restricted = mode.get("macro_present") or mode.get("structure_grade") == "Poor"
            if restricted and spread["distance_points"] < (mode.get("full_size_threshold") or 0.0):
                return 5, "Standard mode keeps base size at 8, but macro or final structure = Poor reduces size to 5 until distance reaches 2x expected move."
            return 8, "Standard mode keeps Apollo's base 8-contract size because the spread satisfies the core restriction stack."

        if mode["key"] == "aggressive":
            if mode.get("allow_scale_up"):
                return 10, "Aggressive mode scales to 10 contracts because no macro is present and structure is Neutral or Good."
            return 8, "Aggressive mode keeps the default 8-contract size because scale-up conditions were not met."

        if mode.get("macro_present") or mode.get("structure_grade") != "Good":
            return 5, "Fortress mode cuts size to 5 because macro is present or final structure is not Good."
        return 8, "Fortress mode keeps 8 contracts because structure is Good and no macro restriction is active."

    def _mode_sort_key(self, mode: Dict[str, Any], item: Dict[str, Any]) -> tuple:
        if mode["key"] == "standard":
            delta = item.get("short_delta")
            return (
                int(item.get("recommended_contract_size") or 0),
                1 if delta is not None and delta <= mode["delta_preferred"] else 0,
                -(abs((delta or 0.0) - mode["delta_preferred"])) if delta is not None else -99.0,
                float(item.get("em_multiple") or 0.0),
                float(item.get("credit") or 0.0),
            )

        if mode["key"] == "aggressive":
            return (
                int(item.get("recommended_contract_size") or 0),
                float(item.get("premium_received_dollars") or 0.0),
                float(item.get("credit") or 0.0),
                -(item.get("short_delta") or 0.0),
            )

        premium_ratio = float(item.get("premium_ratio_to_standard") or 0.0)
        return (
            int(bool(item.get("meets_premium_target_band")) and bool(item.get("meets_black_swan_risk_target"))),
            -abs(premium_ratio - self.FORTRESS_TARGET_PREMIUM_RATIO_IDEAL),
            float(item.get("risk_efficiency") or 0.0),
            float(item.get("premium_received_dollars") or 0.0),
            float(item.get("distance_points") or 0.0),
            -(item.get("short_delta") or 0.0),
            float(item.get("em_multiple") or 0.0),
        )

    def _build_rationale(
        self,
        mode: Dict[str, Any],
        spread: Dict[str, Any],
        contract_size: int,
        contract_size_reason: str,
    ) -> List[str]:
        short_strike = spread["short_put"]["strike"]
        long_strike = spread["long_put"]["strike"]
        rationale = [
            f"{mode['label']} uses {mode['rule_set_label']}.",
            f"The {short_strike:,.0f}/{long_strike:,.0f} spread sits {spread['distance_points']:,.2f} points from spot, or {spread['em_multiple']:,.2f}x expected move.",
            f"Net credit is about {spread['credit']:,.2f} on {spread['width']:,.0f} points of width.",
            contract_size_reason,
        ]
        if mode["key"] == "aggressive":
            rationale.append("Aggressive mode optimizes premium once the 1.5% SPX rule and 1.0x expected-move safety floor are satisfied.")
        elif mode["key"] == "fortress":
            rationale.append("Fortress mode anchors to Standard's EM, builds a fixed 15-point baseline at 10 contracts, then optimizes by credit efficiency, total premium, and max loss.")
        else:
            rationale.append("Standard mode balances Apollo's core delta preference with premium only after the full restriction stack passes.")
        return rationale

    def _build_diagnostics(
        self,
        mode: Dict[str, Any],
        policy: Dict[str, Any],
        spread: Dict[str, Any],
        pricing_context: Dict[str, Any],
        contract_size: int,
        contract_size_reason: str,
        premium_received_dollars: float,
        max_loss_dollars: float,
        distance_pass: bool,
        delta_preferred: bool,
        delta_pass: bool,
        em_buffer: float,
        risk_overlay: Dict[str, Any],
        realistic_max_loss_dollars: float,
    ) -> List[str]:
        items = [
            f"Rule set used: {policy['active_rule_set']}.",
            f"Distance vs rule: {spread['distance_points']:,.2f} points vs EM threshold {policy['distance_threshold']:,.2f} ({'Pass' if distance_pass else 'Fail'}).",
            f"Distance gate: {policy['boundary_rule_used']} | binding source {policy['boundary_binding_source']}.",
            f"Distance vs expected move: {spread['em_multiple']:,.2f}x expected move.",
            f"Pricing basis: {pricing_context['pricing_basis']} | short bid {spread['short_put']['bid']:,.2f} - long ask {spread['long_put']['ask']:,.2f} = {pricing_context['executable_credit']:,.2f}; midpoint model = {pricing_context['mid_credit']:,.2f}; selected net credit = {pricing_context['selected_credit']:,.2f}.",
            f"Premium / contract: ${pricing_context['selected_credit'] * 100:,.0f} | Total premium: ${premium_received_dollars:,.0f}.",
            f"Credit efficiency: {(self._calculate_credit_efficiency_pct(total_premium=premium_received_dollars, total_max_loss=max_loss_dollars) or 0.0):.1f}% using total premium / total max loss.",
            f"Delta compliance: short delta {spread['short_delta']:.2f} | preferred <= {mode['delta_preferred']:.2f} = {'Yes' if delta_preferred else 'No'} | max <= {mode['delta_max']:.2f} = {'Yes' if delta_pass else 'No'}.",
            f"Premium received: ${premium_received_dollars:,.0f} total | Max loss: ${max_loss_dollars:,.0f} total.",
            f"Black Swan Loss: ${realistic_max_loss_dollars:,.0f} using 60% of theoretical max loss for gap risk and real-world exit behavior.",
            f"Contract sizing decision: {contract_size} contracts | {contract_size_reason}",
            f"Risk cap allowed: ${risk_overlay['risk_cap_dollars']:,.0f} | status: {risk_overlay['risk_cap_status']}.",
            f"Original size {risk_overlay['original_contract_size']} -> adjusted size {risk_overlay['adjusted_contract_size']} | {risk_overlay['account_risk_percent']:.2f}% of account at risk.",
            "Exit ladder applied: Yes.",
            f"Break-even buffer vs lower expected-move boundary: {em_buffer:,.2f} points.",
        ]
        if mode["key"] == "fortress" and risk_overlay.get("premium_ratio_to_standard") is not None and risk_overlay.get("standard_total_premium") is not None:
            items.append(
                f"Fortress premium target: ${premium_received_dollars:,.0f} is {(risk_overlay['premium_ratio_to_standard'] or 0.0) * 100:.0f}% of Standard total premium ${risk_overlay['standard_total_premium']:,.0f} (target 40-60%)."
            )
        if mode["key"] == "fortress" and risk_overlay.get("max_loss_ratio_to_standard") is not None and risk_overlay.get("standard_max_loss") is not None:
            items.append(
                f"Fortress risk target: ${max_loss_dollars:,.0f} is {(risk_overlay['max_loss_ratio_to_standard'] or 0.0) * 100:.0f}% of Standard max loss ${risk_overlay['standard_max_loss']:,.0f} (target <=50%)."
            )
        return items

    def _build_no_trade_diagnostics(self, mode: Dict[str, Any], policy: Dict[str, Any], rejection_counts: Counter[str]) -> List[str]:
        items = [
            f"Rule set used: {policy.get('active_rule_set') or mode['rule_set_label']}.",
            self._build_no_trade_message(mode, rejection_counts),
        ]
        if rejection_counts:
            items.append(self._format_rejection_summary(rejection_counts))
        return items

    def _build_no_trade_message(self, mode: Dict[str, Any], rejection_counts: Counter[str]) -> str:
        if mode.get("key") != "fortress":
            return "No valid trade under this mode."
        distance_rule_label = mode.get("strict_policy", {}).get("distance_threshold_label") or "Fortress EM"
        if any(
            "standard reference unavailable" in reason.lower() or "invalid standard-relative reference" in reason.lower()
            for reason in rejection_counts
        ):
            return "No Fortress trade was produced because Standard did not generate a usable reference trade."
        if any("exceeds max loss cap" in reason.lower() for reason in rejection_counts):
            return f"No Fortress trade cleared the {distance_rule_label} safety target while still fitting inside the total max-loss cap."
        if any("standard-relative risk cap" in reason.lower() for reason in rejection_counts):
            return f"No Fortress trade cleared the {distance_rule_label} safety target while staying inside the Standard-relative 50% risk cap."
        if any("premium band" in reason.lower() for reason in rejection_counts):
            return "No Fortress trade could reach the 40-60% Standard premium band while using executable pricing and staying inside the risk target."
        safety_failures = sum(count for reason, count in rejection_counts.items() if "distance rule" in reason.lower())
        efficiency_failures = sum(count for reason, count in rejection_counts.items() if "risk efficiency below" in reason.lower())
        if safety_failures and efficiency_failures:
            return f"No Fortress trade met both the {distance_rule_label} safety boundary and the credit-efficiency ranking rules."
        if safety_failures:
            return f"No Fortress strike met the {distance_rule_label} safety boundary."
        if efficiency_failures:
            return "No Fortress trade met the credit-efficiency ranking rules."
        return "No valid Fortress trade under this mode."

    @staticmethod
    def _format_rejection_summary(rejection_counts: Counter[str]) -> str:
        if not rejection_counts:
            return "Other candidates rejected: no rejection breakdown available."
        summary = "; ".join(f"{label} ({count})" for label, count in rejection_counts.most_common(4))
        return f"Other candidates rejected for: {summary}."

    def _build_expected_move_comparison(self, policy: Dict[str, Any], spread: Dict[str, Any]) -> str:
        return f"{spread['distance_points']:,.2f} points vs {policy['target_em_multiple']:.2f}x EM {policy['em_floor_points']:,.2f}"

    def _estimate_realistic_max_loss(self, max_loss_dollars: float) -> float:
        return max_loss_dollars * self.REALISTIC_MAX_LOSS_FACTOR

    @staticmethod
    def _calculate_risk_efficiency(*, premium_collected: float, maximum_theoretical_risk: float) -> float | None:
        if maximum_theoretical_risk <= 0:
            return None
        return premium_collected / maximum_theoretical_risk

    @staticmethod
    def _build_mode_score(mode: Dict[str, Any], spread: Dict[str, Any], delta_preferred: bool, contract_size: int) -> int:
        base = 50
        if mode["key"] == "aggressive":
            return int(base + min(25, spread["credit"] * 8) + contract_size)
        if mode["key"] == "fortress":
            return int(base + min(25, spread["distance_points"] / 4) + (5 if delta_preferred else 0))
        return int(base + (8 if delta_preferred else 0) + min(15, spread["credit"] * 6))

    def _build_empty_result(
        self,
        option_chain: Dict[str, Any],
        status: str,
        status_class: str,
        message: str,
        expected_move: float | None = None,
        expected_move_range: str = "—",
        underlying_price: float | None = None,
    ) -> Dict[str, Any]:
        return {
            "title": self.TITLE,
            "success": False,
            "status": status,
            "status_class": status_class,
            "message": message,
            "candidate_count": 0,
            "count_label": "3 Modes",
            "valid_mode_count": 0,
            "underlying_price": round(underlying_price, 2) if underlying_price is not None else None,
            "expected_move": round(expected_move, 2) if expected_move is not None else None,
            "expected_move_range": expected_move_range,
            "expiration_date": option_chain.get("expiration_date"),
            "candidates": [],
            "diagnostics": {
                "evaluated_spreads": 0,
                "qualified_spreads": 0,
                "selected_spreads": 0,
                "rejected_spreads": 0,
                "required_distance_mode": "1.5x expected move baseline",
                "baseline_distance_points": None,
                "baseline_max_short_strike": None,
                "expected_move": round(expected_move, 2) if expected_move is not None else None,
                "expected_move_1_5x_threshold": None,
                "expected_move_2x_threshold": None,
                "active_barrier_mode": "1.5x expected move baseline",
                "active_barrier_points": None,
                "macro_modifier_applied": "No",
                "structure_modifier_applied": "No",
                "structure_grade_used": "Not available",
                "base_structure_grade": "Not available",
                "rsi_modifier_applied": "No",
                "rsi_modifier_label": "None",
                "recommended_contract_size_framework": "No candidate sizing framework was applied because Gate 3 could not build mode outputs.",
                "top_rejections": [],
                "mode_summaries": [],
                "evaluated_spread_details": [],
            },
        }

    @staticmethod
    def _build_exit_plan(short_strike: float | None, long_strike: float | None) -> List[str]:
        short_label = f"{short_strike:,.0f}" if short_strike is not None else "the short strike"
        long_label = f"{long_strike:,.0f}" if long_strike is not None else "the long strike"
        return [
            f"If SPX moves within 30 points of the short strike at {short_label}, close 25%.",
            f"If SPX moves within 15 points of the short strike at {short_label}, close another 25%.",
            f"If two consecutive 5-minute candles close fully beyond the short strike at {short_label}, close another 25%.",
            f"If the long strike at {long_label} is touched, close the remaining 25%.",
        ]

    def _apply_risk_overlay(
        self,
        mode: Dict[str, Any],
        width: float,
        credit: float,
        original_contract_size: int,
        base_reason: str,
    ) -> Dict[str, Any]:
        risk_cap_dollars = self._risk_cap_dollars(mode["key"])
        per_contract_max_loss = max((width - credit) * 100, 0.0)
        if per_contract_max_loss <= 0:
            adjusted_contract_size = original_contract_size
        else:
            adjusted_contract_size = min(original_contract_size, int(risk_cap_dollars // per_contract_max_loss))

        if adjusted_contract_size < 1:
            return {
                "valid": False,
                "adjusted_for_cap": False,
                "original_contract_size": original_contract_size,
                "adjusted_contract_size": 0,
                "contract_size_reason": f"{base_reason} Rejected: exceeds max loss cap even at 1 contract.",
                "risk_cap_dollars": risk_cap_dollars,
                "risk_cap_status": "Rejected: exceeds max loss cap",
                "premium_received_dollars": 0.0,
                "max_loss_dollars": per_contract_max_loss,
                "realistic_max_loss_dollars": self._estimate_realistic_max_loss(per_contract_max_loss),
                "account_risk_percent": (per_contract_max_loss / self.account_value) * 100 if self.account_value else None,
            }

        adjusted_for_cap = adjusted_contract_size < original_contract_size
        premium_received_dollars = credit * 100 * adjusted_contract_size
        max_loss_dollars = per_contract_max_loss * adjusted_contract_size
        risk_cap_status = "Adjusted for risk cap" if adjusted_for_cap else "Within risk cap"
        reason = base_reason
        if adjusted_for_cap:
            reason = f"{base_reason} Reduced from {original_contract_size} to {adjusted_contract_size} contracts to stay within the max loss cap."
        return {
            "valid": True,
            "adjusted_for_cap": adjusted_for_cap,
            "original_contract_size": original_contract_size,
            "adjusted_contract_size": adjusted_contract_size,
            "contract_size_reason": reason,
            "risk_cap_dollars": risk_cap_dollars,
            "risk_cap_status": risk_cap_status,
            "premium_received_dollars": premium_received_dollars,
            "max_loss_dollars": max_loss_dollars,
            "realistic_max_loss_dollars": self._estimate_realistic_max_loss(max_loss_dollars),
            "account_risk_percent": (max_loss_dollars / self.account_value) * 100 if self.account_value else None,
        }

    def _build_invalid_candidate(
        self,
        mode: Dict[str, Any],
        policy: Dict[str, Any],
        spread: Dict[str, Any],
        pricing_context: Dict[str, Any],
        risk_overlay: Dict[str, Any],
        log: Dict[str, Any],
    ) -> Dict[str, Any]:
        short_put = spread["short_put"]
        long_put = spread["long_put"]
        selected_credit = float(pricing_context.get("selected_credit") or 0.0)
        per_contract_max_loss = max((spread["width"] - selected_credit) * 100, 0.0)
        no_trade_message = f"{risk_overlay.get('risk_cap_status') or 'Rejected by risk overlay'}."
        return {
            "strategy_label": f"Sell {int(short_put['strike'])} / Buy {int(long_put['strike'])} put credit spread",
            "short_strike": short_put["strike"],
            "long_strike": long_put["strike"],
            "width": spread["width"],
            "credit": round(selected_credit, 2),
            "premium_per_contract": round(selected_credit * 100, 2),
            "premium_received_dollars": 0.0,
            "total_premium": 0.0,
            "max_theoretical_risk": round(risk_overlay["max_loss_dollars"], 2),
            "max_theoretical_risk_per_contract": round(per_contract_max_loss, 2),
            "risk_efficiency": round(
                self._calculate_risk_efficiency(
                    premium_collected=selected_credit * 100,
                    maximum_theoretical_risk=per_contract_max_loss,
                )
                or 0.0,
                4,
            ),
            "credit_efficiency_pct": 0.0,
            "max_loss": round(risk_overlay["max_loss_dollars"], 2),
            "max_loss_per_contract": round(max((spread["width"] - selected_credit), 0.0), 2),
            "break_even": round(spread["break_even"], 2),
            "short_delta": round(spread["short_delta"], 2) if spread["short_delta"] is not None else None,
            "long_delta": round(spread["long_delta"], 2) if spread.get("long_delta") is not None else None,
            "distance_points": round(spread["distance_points"], 2),
            "distance_percent": round(spread["distance_percent"] * 100, 2),
            "expected_move": round(spread["expected_move"], 2),
            "expected_move_used": round(spread["expected_move"], 2),
            "expected_move_source": "atm_straddle_estimate",
            "expected_move_1_5x_threshold": round(spread["expected_move"] * 1.5, 2),
            "expected_move_2x_threshold": round(spread["expected_move"] * 2.0, 2),
            "expected_move_comparison": self._build_expected_move_comparison(policy=policy, spread=spread),
            "required_distance_rule_used": policy["distance_threshold_label"],
            "active_em_rule": policy["active_rule_set"],
            "active_rule_set": policy["active_rule_set"],
            "target_em_multiple": round(policy["target_em_multiple"], 2),
            "target_em": round(policy["target_em_multiple"], 2),
            "applied_em_multiple_floor": round(policy["applied_em_multiple_floor"], 2),
            "percent_floor": round(policy["percent_floor"], 2) if policy.get("percent_floor") is not None else None,
            "percent_floor_points": round(policy["percent_floor_points"], 2) if policy.get("percent_floor_points") is not None else None,
            "em_floor_points": round(policy["em_floor_points"], 2),
            "hybrid_distance_threshold": round(policy["distance_threshold"], 2),
            "boundary_binding_source": policy["boundary_binding_source"],
            "boundary_rule_used": policy["boundary_rule_used"],
            "pass_type": policy["pass_type"],
            "pass_type_label": policy.get("pass_type_label") or policy["pass_type"],
            "fallback_used": policy["fallback_used"],
            "fallback_rule_name": policy["fallback_rule_name"],
            "recommended_contract_size": 0,
            "recommended_contract_size_reason": risk_overlay["contract_size_reason"],
            "score": 0,
            "rationale": [
                f"{mode['label']} candidate failed the account-level risk overlay.",
                f"{risk_overlay.get('risk_cap_status') or 'Risk overlay rejection'} at 1 contract against a cap of ${risk_overlay['risk_cap_dollars']:,.0f}.",
            ],
            "exit_plan": self._build_exit_plan(short_put["strike"], long_put["strike"]),
            "diagnostics": [
                f"Rule set used: {mode['rule_set_label']}.",
                f"Pricing basis: {pricing_context['pricing_basis']} | selected net credit = {selected_credit:,.2f}.",
                f"Rejected by risk overlay: {risk_overlay.get('risk_cap_status') or 'Risk overlay rejection'} at a cap of ${risk_overlay['risk_cap_dollars']:,.0f}.",
                "Exit ladder applied: Yes.",
            ],
            "short_open_interest": round(spread["short_open_interest"]),
            "short_volume": round(spread["short_volume"]),
            "short_bid": round(short_put["bid"], 2),
            "short_ask": round(short_put["ask"], 2),
            "long_bid": round(long_put["bid"], 2),
            "long_ask": round(long_put["ask"], 2),
            "qualifies_for_full_size": False,
            "distance_above_threshold": round(spread["distance_points"] - policy["distance_threshold"], 2),
            "em_buffer": round((spread["spot"] - spread["expected_move"]) - spread["break_even"], 2),
            "em_multiple": round(spread["em_multiple"], 2),
            "actual_distance_to_short": round(spread["distance_points"], 2),
            "actual_em_multiple": round(spread["em_multiple"], 2),
            "risk_cap_dollars": round(risk_overlay["risk_cap_dollars"], 2),
            "risk_cap_status": risk_overlay["risk_cap_status"],
            "risk_cap_adjusted": False,
            "original_contract_size": risk_overlay["original_contract_size"],
            "adjusted_contract_size": 0,
            "account_risk_percent": round(risk_overlay["account_risk_percent"], 2) if risk_overlay["account_risk_percent"] is not None else None,
            "realistic_max_loss": round(risk_overlay["realistic_max_loss_dollars"], 2),
            "exit_plan_applied": True,
            "available": False,
            "no_trade_message": no_trade_message,
        }

    def _risk_cap_dollars(self, mode_key: str) -> float:
        return self.account_value * self.MODE_MAX_LOSS_CAPS.get(mode_key, 0.15)

    @staticmethod
    def _normalize_structure_grade(value: Any) -> str:
        normalized = str(value or "Not available").title()
        if normalized not in {"Good", "Neutral", "Poor"}:
            return "Not available"
        return normalized

    @staticmethod
    def _normalize_macro_grade(value: Any) -> str:
        normalized = str(value or "None").title()
        if normalized not in {"None", "Minor", "Major"}:
            return "None"
        return normalized

    def _estimate_expected_move(
        self,
        puts: List[Dict[str, Any]],
        calls: List[Dict[str, Any]],
        spot: float | None,
    ) -> float | None:
        if spot is None:
            return None
        nearest_put = self._find_nearest_contract(puts, spot)
        nearest_call = self._find_nearest_contract(calls, spot)
        if nearest_put is not None and nearest_call is not None:
            estimate = self._premium_anchor(nearest_put) + self._premium_anchor(nearest_call)
            return round(estimate, 2) if estimate > 0 else None
        if nearest_put is not None:
            estimate = self._premium_anchor(nearest_put) * 2
            return round(estimate, 2) if estimate > 0 else None
        if nearest_call is not None:
            estimate = self._premium_anchor(nearest_call) * 2
            return round(estimate, 2) if estimate > 0 else None
        return None

    @staticmethod
    def _find_nearest_contract(contracts: List[Dict[str, Any]], spot: float) -> Dict[str, Any] | None:
        if not contracts:
            return None
        return min(contracts, key=lambda item: abs(item.get("strike", 0.0) - spot))

    def _normalize_contracts(self, contracts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for contract in contracts:
            strike = self._coerce_float(contract.get("strike"))
            if strike is None:
                continue
            delta = self._coerce_float(contract.get("delta"))
            normalized.append(
                {
                    "symbol": contract.get("symbol"),
                    "put_call": contract.get("put_call"),
                    "strike": strike,
                    "bid": self._coerce_float(contract.get("bid")) or 0.0,
                    "ask": self._coerce_float(contract.get("ask")) or 0.0,
                    "last": self._coerce_float(contract.get("last")) or 0.0,
                    "mark": self._coerce_float(contract.get("mark")) or 0.0,
                    "delta": delta,
                    "delta_abs": abs(delta) if delta is not None else None,
                    "open_interest": self._coerce_float(contract.get("open_interest")) or 0.0,
                    "total_volume": self._coerce_float(contract.get("total_volume")) or 0.0,
                }
            )
        return sorted(normalized, key=lambda item: item["strike"])

    @staticmethod
    def _build_expected_move_range(spot: float | None, expected_move: float | None) -> str:
        if spot is None or expected_move is None:
            return "—"
        return f"{spot - expected_move:,.2f} to {spot + expected_move:,.2f}"

    @staticmethod
    def _floor_to_valid_strike(raw_threshold: float | None, available_strikes: List[float]) -> float | None:
        if raw_threshold is None:
            return None
        valid = sorted({round(value, 2) for value in available_strikes if value is not None and value <= raw_threshold})
        if not valid:
            return None
        return valid[-1]

    @staticmethod
    def _premium_anchor(contract: Dict[str, Any]) -> float:
        bid = float(contract.get("bid") or 0.0)
        ask = float(contract.get("ask") or 0.0)
        mark = float(contract.get("mark") or 0.0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return max(mark, bid, ask)

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
