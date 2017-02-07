# -*- coding: utf-8 -*-
import jsonschema
import os

from dmscripts.helpers.csv_helpers import make_field_title, count_field_in_record, MultiCSVWriter
from dmscripts.helpers.framework_helpers import find_suppliers_with_details_and_draft_service_counts


DRAFT_STATUSES = [
    'completed',
    'failed',
    'draft',
]


def get_validation_errors(candidate, schema):
    validator = jsonschema.Draft4Validator(schema)
    errors = validator.iter_errors(candidate)
    error_keys = [error.path[0] for error in errors]
    return error_keys


def fields_from_content_question_gen(question, record):
    if question["type"] == "checkboxes":
        for option in question.options:
            # Make a CSV column for each label
            yield (
                make_field_title(question.id, option["label"]),
                count_field_in_record(question.id, option["label"], record)
            )
    elif question.fields:
        for field_id in sorted(question.fields.values()):
            # Make a CSV column containing all values
            yield (
                field_id,
                "|".join(service.get(field_id, "") for service in record["services"])
            )
    else:
        yield (
            question["id"],
            "|".join(str(service.get(question["id"], "")) for service in record["services"])
        )


def get_declaration_questions(declaration_content, record):
    for section in declaration_content:
        for question in section.questions:
            yield question, record['declaration'].get(question.id)


def add_failed_questions(declaration_content,
                         declaration_definite_pass_schema,
                         declaration_baseline_schema
                         ):
    def inner(record):
        if record['declaration'].get('status') != 'complete':
            return dict(record,
                        failed_mandatory=['INCOMPLETE'],
                        discretionary=[])

        declaration_questions = list(get_declaration_questions(declaration_content, record))

        all_failed_keys = get_validation_errors(record['declaration'], declaration_definite_pass_schema)

        if declaration_baseline_schema:
            baseline_only_failed_keys = get_validation_errors(record['declaration'], declaration_baseline_schema)
        else:
            baseline_only_failed_keys = all_failed_keys

        failed_mandatory = [
            "Q{} - {}".format(question.number, question.id)
            for question, answer in declaration_questions
            if question.id in baseline_only_failed_keys
            ]
        discretionary = [
            ("Q{} - {}".format(question.number, question.id), answer)
            for question, answer in declaration_questions
            if question.id in all_failed_keys and question.id not in baseline_only_failed_keys
        ]

        return dict(record,
                    failed_mandatory=failed_mandatory,
                    discretionary=discretionary)

    return inner


def find_suppliers_with_details(client,
                                content_loader,
                                framework_slug,
                                declaration_definite_pass_schema,
                                declaration_baseline_schema,
                                supplier_ids=None
                                ):

    content_loader.load_manifest(framework_slug, 'declaration', 'declaration')
    declaration_content = content_loader.get_manifest(framework_slug, 'declaration')

    records = find_suppliers_with_details_and_draft_service_counts(client, framework_slug, supplier_ids)
    records = map(add_failed_questions(declaration_content,
                                       declaration_definite_pass_schema,
                                       declaration_baseline_schema), records)

    return records


def supplier_info(record):
    return [
        ('supplier_name', record['supplier']['name']),
        ('supplier_id', record['supplier']['id']),
    ]


def failed_mandatory_questions(record):
    return [
        ('failed_mandatory', ",".join(question for question in record['failed_mandatory'])),
    ]


def discretionary_questions(record):
    failed_questions = [("failed_discretionary", record['discretionary'])]
    mitigating_factors = [
        ("mitigating factors 1", record['declaration'].get('mitigatingFactors', '')),
        ("mitigating factors 2", record['declaration'].get('mitigatingFactors2', ''))
    ]
    return failed_questions + mitigating_factors


def contact_details(record):
    return [
        ('contact_name', record['declaration'].get('primaryContact', '')),
        ('contact_email', record['declaration'].get('primaryContactEmail', '')),
    ]


class SuccessfulHandler(object):
    NAME = 'successful'

    def matches(self, record):
        return record['onFramework'] is True

    def should_write(self, record):
        return True

    def create_row(self, record):
        return \
            supplier_info(record) + \
            contact_details(record)


class FailedHandler(object):
    NAME = 'failed'

    def matches(self, record):
        return record['onFramework'] is False

    def should_write(self, record):
        # Only include failed complete applications, not people who didn't make an application
        if record.get('failed_mandatory') and 'INCOMPLETE' in record.get('failed_mandatory'):
            return False
        draft_counts = record.get('counts')
        # Keys in the Counter are tuples such as ('digital-specialists', 'submitted') so count[1] will match the status
        # Failed services are also "submitted" and "complete" so count these too
        completed_count = sum(
            draft_counts[key] for key in
            [count for count in draft_counts if count[1] in ['submitted', 'failed']]
        )
        if completed_count == 0:
            return False
        if not record.get('failed_mandatory'):
            record['failed_mandatory'] = ['No passed lot']
        return True

    def create_row(self, record):
        return \
            supplier_info(record) + \
            failed_mandatory_questions(record) + \
            contact_details(record)


class DiscretionaryHandler(object):
    NAME = 'discretionary'

    def matches(self, record):
        return record['onFramework'] is None

    def should_write(self, record):
        return True

    def create_row(self, record):
        return \
            supplier_info(record) + \
            discretionary_questions(record) + \
            contact_details(record)


def export_suppliers(
        client,
        framework_slug,
        content_loader,
        output_dir,
        declaration_definite_pass_schema,
        declaration_baseline_schema=None,
        supplier_ids=None,
):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    records = find_suppliers_with_details(
        client,
        content_loader,
        framework_slug,
        declaration_definite_pass_schema,
        declaration_baseline_schema,
        supplier_ids
    )

    handlers = [SuccessfulHandler(), FailedHandler(), DiscretionaryHandler()]

    with MultiCSVWriter(output_dir, handlers) as writer:
        for i, record in enumerate(records):
            writer.write_row(record)
            if (i + 1) % 100 == 0:
                writer.print_counts()

        writer.print_counts()
        print("DONE")
