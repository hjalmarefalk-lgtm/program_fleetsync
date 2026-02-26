"""Validation severity helpers and shared validation utilities."""

from __future__ import annotations

from typing import Iterable, List

from .models import ValidationIssue, ValidationReport


def issue(code: str, severity: str, message: str, path: str | None = None) -> ValidationIssue:
    """Create a ValidationIssue with explicit severity."""
    return ValidationIssue(code=code, severity=severity, message=message, path=path)


def compute_valid(warnings: Iterable[ValidationIssue], fatals: Iterable[ValidationIssue]) -> bool:
    """Return True only if no fatal issues are present."""
    return len(list(fatals)) == 0


def merge_reports(*reports: ValidationReport) -> ValidationReport:
    """Merge multiple ValidationReports into one."""
    warnings: List[ValidationIssue] = []
    fatals: List[ValidationIssue] = []
    for report in reports:
        warnings.extend(report.warnings)
        fatals.extend(report.fatals)
    return ValidationReport(valid=compute_valid(warnings, fatals), warnings=warnings, fatals=fatals)
