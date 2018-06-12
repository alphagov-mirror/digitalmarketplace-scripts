#!/usr/bin/env python3
"""

PREREQUISITE: You'll need wkhtmltopdf installed for this to work (http://wkhtmltopdf.org/). The `default.nix` in this
repo can provide this dependency.

Generate framework agreement counterpart signature pages from supplier "about you" information for suppliers
who successfully applied to a framework and whose signed agreements have been approved by CCS.

The 'frameworkAgreementDetails' JSON object from the API should look something like this:
{
    "contractNoticeNumber": "2016/S 217-395765",
    "frameworkAgreementVersion": "RM1043iv",
    "frameworkExtensionLength": "12 months",
    "frameworkRefDate": "16-01-2017",
    "frameworkURL": "https://www.gov.uk/government/publications/digital-outcomes-and-specialists-2-framework-agreement",
    "lotDescriptions": {
      "digital-outcomes": "Lot 1: digital outcomes",
      "digital-specialists": "Lot 2: digital specialists",
      "user-research-participants": "Lot 4: user research participants",
      "user-research-studios": "Lot 3: user research studios"
    },
    "lotOrder": [
      "digital-outcomes",
      "digital-specialists",
      "user-research-studios",
      "user-research-participants"
    ],
    "pageTotal": 45,
    "signaturePageNumber": 3
}

Usage:
    scripts/generate-framework-agreement-counterpart-signature-pages.py <stage> <api_token> <framework_slug>
    <template_folder> <output_folder> [<supplier_id_file>]

"""
import os
import shutil
import sys
import tempfile

sys.path.insert(0, '.')  # noqa

from docopt import docopt
from dmscripts.export_framework_applicant_details import get_csv_rows
from dmscripts.helpers.env_helpers import get_api_endpoint_from_stage
from dmscripts.helpers.framework_helpers import find_suppliers_with_details_and_draft_service_counts
from dmscripts.helpers.supplier_data_helpers import get_supplier_ids_from_file
from dmscripts.generate_framework_agreement_signature_pages import (
    render_html_for_suppliers_awaiting_countersignature, render_pdf_for_each_html_page
)
from dmapiclient import DataAPIClient


if __name__ == '__main__':
    args = docopt(__doc__)

    framework_slug = args['<framework_slug>']
    client = DataAPIClient(get_api_endpoint_from_stage(args['<stage>']), args['<api_token>'])
    framework = client.get_framework(framework_slug)['frameworks']
    framework_lot_slugs = tuple([lot['slug'] for lot in client.get_framework(framework_slug)['frameworks']['lots']])
    framework_kwargs = framework['frameworkAgreementDetails']
    framework_kwargs['frameworkName'] = framework['name']

    supplier_id_file = args['<supplier_id_file>']
    supplier_ids = get_supplier_ids_from_file(supplier_id_file)
    html_dir = tempfile.mkdtemp()

    records = find_suppliers_with_details_and_draft_service_counts(client, framework_slug, supplier_ids)
    headers, rows = get_csv_rows(records, framework_slug, framework_lot_slugs, count_statuses=("submitted",))
    render_html_for_suppliers_awaiting_countersignature(rows, framework_kwargs, args['<template_folder>'], html_dir)
    html_pages = os.listdir(html_dir)
    html_pages.remove('framework-agreement-signature-page.css')
    html_pages.remove('framework-agreement-countersignature.png')
    ok = render_pdf_for_each_html_page(html_pages, html_dir, args['<output_folder>'])
    shutil.rmtree(html_dir)
    if not ok:
        sys.exit(1)
