from unicodecsv import reader

import pytest
from freezegun import freeze_time
from mock import Mock, patch

from dmscripts.export_framework_results_reasons import export_suppliers

from .assessment_helpers import BaseAssessmentTest, BaseAssessmentOnFrameworksAsThoughNoBaselineTestMixin


class _BaseExportFrameworkResultsReasonsTest(BaseAssessmentTest):
    def _mock_get_questions_numbers_from_framework_impl(self, framework_slug, content_loader):
        assert content_loader is self.mock_content_loader
        assert framework_slug == self.framework_slug
        return {
            question_id: question_number
            for question_number, question_id in enumerate(self._get_ordered_question_ids())
        }

    def setup_method(self, method):
        r = super(_BaseExportFrameworkResultsReasonsTest, self).setup_method(method)

        self.mock_content_loader = Mock()
        self.get_questions_numbers_from_framework_patcher = patch(
            "dmscripts.export_framework_results_reasons.get_questions_numbers_from_framework",
        )
        self.mock_get_questions_numbers_from_framework = self.get_questions_numbers_from_framework_patcher.start()
        self.mock_get_questions_numbers_from_framework.side_effect = \
            self._mock_get_questions_numbers_from_framework_impl

        return r


class _BaseExportTest(_BaseExportFrameworkResultsReasonsTest):
    # implemented as an "inner" function so it can be parametrized differently in different subclasses
    def _test_export_inner(
        self,
        tmpdir,
        use_baseline_schema,
        expected_failed,
        expected_discretionary,
        expected_successful,
    ):
        with freeze_time('2019-01-01 12:01:23'):
            export_suppliers(
                self.mock_data_client,
                self.framework_slug,
                self.mock_content_loader,
                str(tmpdir.join("doesnt.exist.yet")),
                self._declaration_definite_pass_schema(),
                self._declaration_definite_pass_schema()["definitions"]["baseline"] if use_baseline_schema else None,
            )

        failed_filename = "h-cloud-99-suppliers-who-failed-2019-01-01-12-01.csv"
        successful_filename = "h-cloud-99-automatically-successful-suppliers-2019-01-01-12-01.csv"
        discretionary_filename = "h-cloud-99-suppliers-who-declared-discretionary-data-2019-01-01-12-01.csv"
        assert frozenset(p.basename for p in tmpdir.join("doesnt.exist.yet").listdir()) == frozenset((
            failed_filename,
            discretionary_filename,
            successful_filename,
        ))

        with tmpdir.join("doesnt.exist.yet", failed_filename).open("rb") as f:
            freader = reader(f, encoding="utf-8")
            lines = tuple(freader)
            assert lines[0] == [
                "supplier_name",
                "supplier_id",
                "failed_mandatory",
                "contact_name",
                "contact_email",
                "admin_link",
            ]
            assert sorted(lines[1:]) == sorted(expected_failed)

        with tmpdir.join("doesnt.exist.yet", discretionary_filename).open("rb") as f:
            freader = reader(f, encoding="utf-8")
            lines = tuple(freader)
            assert lines[0] == [
                "supplier_name",
                "supplier_id",
                "failed_discretionary",
                "mitigating factors 1",
                "mitigating factors 2",
                "mitigating factors 3",
                "contact_name",
                "contact_email",
                "admin_link",
            ]
            assert sorted(lines[1:]) == sorted(expected_discretionary)

        with tmpdir.join("doesnt.exist.yet", successful_filename).open("rb") as f:
            freader = reader(f, encoding="utf-8")
            lines = tuple(freader)
            assert lines[0] == [
                "supplier_name",
                "supplier_id",
                "contact_name",
                "contact_email",
            ]
            assert sorted(lines[1:]) == sorted(expected_successful)


class TestExportOnFrameworkBaseline(_BaseExportTest):
    """
        Tests run against mock data with onFramework set as though a baseline schema was provided when results
        were marked
    """
    @pytest.mark.parametrize("use_baseline_schema,expected_failed,expected_discretionary,expected_successful", (
        (
            True,
            (
                [
                    "Supplier 2345 generic name",
                    "2345",
                    "Q1 - shouldBeFalseStrict",
                    u"Supplier 2345 Empl\u00f6yee 123",
                    "supplier.2345.h-cloud-99@example.com",
                    'https://www.digitalmarketplace.service.gov.uk/admin/suppliers/2345/edit/declarations/h-cloud-99',
                ],
                [
                    "Supplier 3456 generic name",
                    "3456",
                    "No passed lot",
                    u"Supplier 3456 Empl\u00f6yee 123",
                    "supplier.3456.h-cloud-99@example.com",
                    'https://www.digitalmarketplace.service.gov.uk/admin/suppliers/3456/edit/declarations/h-cloud-99',
                ],
            ),
            (
                [
                    "Supplier 1234 generic name",
                    "1234",
                    "[('Q0 - shouldBeFalseLax', True)]",
                    u"Permit, brevi manu, Supplier 1234\u2019s sight is somewhat troubled",
                    "Supplier 1234 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 1234's dog ate their homework",
                    u"Supplier 1234 Empl\u00f6yee 123",
                    "supplier.1234.h-cloud-99@example.com",
                    'https://www.digitalmarketplace.service.gov.uk/admin/suppliers/1234/edit/declarations/h-cloud-99',
                ],
                [
                    "Supplier 7654 generic name",
                    "7654",
                    "[('Q0 - shouldBeFalseLax', True)]",
                    u"Permit, brevi manu, Supplier 7654\u2019s sight is somewhat troubled",
                    "Supplier 7654 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 7654's dog ate their homework",
                    u"Supplier 7654 Empl\u00f6yee 123",
                    "supplier.7654.h-cloud-99@example.com",
                    'https://www.digitalmarketplace.service.gov.uk/admin/suppliers/7654/edit/declarations/h-cloud-99',
                ],
            ),
            (
                [
                    "Supplier 4321 generic name",
                    "4321",
                    u"Supplier 4321 Empl\u00f6yee 123",
                    "supplier.4321.h-cloud-99@example.com",
                ],
                [
                    "Supplier 8765 generic name",
                    "8765",
                    u"Supplier 8765 Empl\u00f6yee 123",
                    "supplier.8765.h-cloud-99@example.com",
                ],
            ),
        ),
        (  # some results may appear odd because onFramework is set as though there was a baseline schema, but we're not
           # supplying that baseline schema to export_suppliers in this parametrization
            False,
            (
                [
                    "Supplier 2345 generic name",
                    "2345",
                    "Q1 - shouldBeFalseStrict",
                    u"Supplier 2345 Empl\u00f6yee 123",
                    "supplier.2345.h-cloud-99@example.com",
                    'https://www.digitalmarketplace.service.gov.uk/admin/suppliers/2345/edit/declarations/h-cloud-99',
                ],
                [
                    "Supplier 3456 generic name",
                    "3456",
                    "No passed lot",
                    u"Supplier 3456 Empl\u00f6yee 123",
                    "supplier.3456.h-cloud-99@example.com",
                    'https://www.digitalmarketplace.service.gov.uk/admin/suppliers/3456/edit/declarations/h-cloud-99',
                ],
            ),
            (
                [
                    "Supplier 1234 generic name",
                    "1234",
                    "[]",
                    u"Permit, brevi manu, Supplier 1234\u2019s sight is somewhat troubled",
                    "Supplier 1234 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 1234's dog ate their homework",
                    u"Supplier 1234 Empl\u00f6yee 123",
                    "supplier.1234.h-cloud-99@example.com",
                    'https://www.digitalmarketplace.service.gov.uk/admin/suppliers/1234/edit/declarations/h-cloud-99',
                ],
                [
                    "Supplier 7654 generic name",
                    "7654",
                    "[]",
                    u"Permit, brevi manu, Supplier 7654\u2019s sight is somewhat troubled",
                    "Supplier 7654 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 7654's dog ate their homework",
                    u"Supplier 7654 Empl\u00f6yee 123",
                    "supplier.7654.h-cloud-99@example.com",
                    'https://www.digitalmarketplace.service.gov.uk/admin/suppliers/7654/edit/declarations/h-cloud-99',
                ],
            ),
            (
                [
                    "Supplier 4321 generic name",
                    "4321",
                    u"Supplier 4321 Empl\u00f6yee 123",
                    "supplier.4321.h-cloud-99@example.com",
                ],
                [
                    "Supplier 8765 generic name",
                    "8765",
                    u"Supplier 8765 Empl\u00f6yee 123",
                    "supplier.8765.h-cloud-99@example.com",
                ],
            ),
        ),
    ))
    def test_export(self, tmpdir, use_baseline_schema, expected_failed, expected_discretionary, expected_successful):
        return self._test_export_inner(
            tmpdir,
            use_baseline_schema,
            expected_failed,
            expected_discretionary,
            expected_successful,
        )


class TestExportOnFrameworkNoBaseline(BaseAssessmentOnFrameworksAsThoughNoBaselineTestMixin, _BaseExportTest):
    """
        Tests run against mock data with onFramework set as though a baseline schema was NOT provided when results
        were marked
    """
    @pytest.mark.parametrize("use_baseline_schema,expected_failed,expected_discretionary,expected_successful", (
        (  # some results may appear odd because onFramework is set as though there was no baseline schema, but we ARE
           # supplying that baseline schema to export_suppliers in this parametrization
            True,
            (
                [
                    "Supplier 3456 generic name",
                    "3456",
                    "No passed lot",
                    u"Supplier 3456 Empl\u00f6yee 123",
                    "supplier.3456.h-cloud-99@example.com",
                    "https://www.digitalmarketplace.service.gov.uk/admin/suppliers/3456/edit/declarations/h-cloud-99"
                ],
            ),
            (
                [
                    "Supplier 1234 generic name",
                    "1234",
                    "[('Q0 - shouldBeFalseLax', True)]",
                    u"Permit, brevi manu, Supplier 1234\u2019s sight is somewhat troubled",
                    "Supplier 1234 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 1234's dog ate their homework",
                    u"Supplier 1234 Empl\u00f6yee 123",
                    "supplier.1234.h-cloud-99@example.com",
                    "https://www.digitalmarketplace.service.gov.uk/admin/suppliers/1234/edit/declarations/h-cloud-99",
                ],
                [
                    "Supplier 2345 generic name",
                    "2345",
                    "[]",
                    u"Permit, brevi manu, Supplier 2345\u2019s sight is somewhat troubled",
                    "Supplier 2345 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 2345's dog ate their homework",
                    u"Supplier 2345 Empl\u00f6yee 123",
                    "supplier.2345.h-cloud-99@example.com",
                    "https://www.digitalmarketplace.service.gov.uk/admin/suppliers/2345/edit/declarations/h-cloud-99",
                ],
                [
                    "Supplier 7654 generic name",
                    "7654",
                    "[('Q0 - shouldBeFalseLax', True)]",
                    u"Permit, brevi manu, Supplier 7654\u2019s sight is somewhat troubled",
                    "Supplier 7654 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 7654's dog ate their homework",
                    u"Supplier 7654 Empl\u00f6yee 123",
                    "supplier.7654.h-cloud-99@example.com",
                    "https://www.digitalmarketplace.service.gov.uk/admin/suppliers/7654/edit/declarations/h-cloud-99",
                ],
            ),
            (
                [
                    "Supplier 4321 generic name",
                    "4321",
                    u"Supplier 4321 Empl\u00f6yee 123",
                    "supplier.4321.h-cloud-99@example.com",
                ],
                [
                    "Supplier 8765 generic name",
                    "8765",
                    u"Supplier 8765 Empl\u00f6yee 123",
                    "supplier.8765.h-cloud-99@example.com",
                ],
            ),
        ),
        (
            False,
            (
                [
                    "Supplier 3456 generic name",
                    "3456",
                    "No passed lot",
                    u"Supplier 3456 Empl\u00f6yee 123",
                    "supplier.3456.h-cloud-99@example.com",
                    "https://www.digitalmarketplace.service.gov.uk/admin/suppliers/3456/edit/declarations/h-cloud-99",
                ],
            ),
            (
                [
                    "Supplier 1234 generic name",
                    "1234",
                    "[]",
                    u"Permit, brevi manu, Supplier 1234\u2019s sight is somewhat troubled",
                    "Supplier 1234 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 1234's dog ate their homework",
                    u"Supplier 1234 Empl\u00f6yee 123",
                    "supplier.1234.h-cloud-99@example.com",
                    "https://www.digitalmarketplace.service.gov.uk/admin/suppliers/1234/edit/declarations/h-cloud-99",

                ],
                [
                    "Supplier 2345 generic name",
                    "2345",
                    "[]",
                    u"Permit, brevi manu, Supplier 2345\u2019s sight is somewhat troubled",
                    "Supplier 2345 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 2345's dog ate their homework",
                    u"Supplier 2345 Empl\u00f6yee 123",
                    "supplier.2345.h-cloud-99@example.com",
                    "https://www.digitalmarketplace.service.gov.uk/admin/suppliers/2345/edit/declarations/h-cloud-99",

                ],
                [
                    "Supplier 7654 generic name",
                    "7654",
                    "[]",
                    u"Permit, brevi manu, Supplier 7654\u2019s sight is somewhat troubled",
                    "Supplier 7654 can scarcely be prepared for every emergency that might crop up",
                    "Supplier 7654's dog ate their homework",
                    u"Supplier 7654 Empl\u00f6yee 123",
                    "supplier.7654.h-cloud-99@example.com",
                    "https://www.digitalmarketplace.service.gov.uk/admin/suppliers/7654/edit/declarations/h-cloud-99",

                ],
            ),
            (
                [
                    "Supplier 4321 generic name",
                    "4321",
                    u"Supplier 4321 Empl\u00f6yee 123",
                    "supplier.4321.h-cloud-99@example.com",
                ],
                [
                    "Supplier 8765 generic name",
                    "8765",
                    u"Supplier 8765 Empl\u00f6yee 123",
                    "supplier.8765.h-cloud-99@example.com",
                ],
            ),
        ),
    ))
    def test_export(self, tmpdir, use_baseline_schema, expected_failed, expected_discretionary, expected_successful):
        return self._test_export_inner(
            tmpdir,
            use_baseline_schema,
            expected_failed,
            expected_discretionary,
            expected_successful,
        )


class TestExportUnexpectedValidationError(_BaseExportTest):
    def _get_supplier_frameworks(self):
        sfs = super()._get_supplier_frameworks()
        del sfs[1234]["declaration"]["omnipresent"]
        return sfs

    @pytest.mark.parametrize("use_baseline_schema", (False, True,))
    def test_failed_export(self, tmpdir, use_baseline_schema):
        with pytest.raises(ValueError, match=r".*Unexpected validation error.*omnipresent.*"):
            return self._test_export_inner(
                tmpdir,
                use_baseline_schema,
                [],
                [],
                [],
            )
