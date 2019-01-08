import mock

from freezegun import freeze_time
import pytest

from dmscripts.data_retention_remove_user_data import data_retention_remove_user_data


@pytest.mark.parametrize("dry_run", (False, True,))
@pytest.mark.parametrize("with_mailchimp", (False, True,))
@mock.patch('dmutils.email.dm_mailchimp.DMMailChimpClient', autospec=True)
@mock.patch('dmapiclient.DataAPIClient', autospec=True)
def test_data_retention_remove_user_data_happy_paths(data_api_client, dm_mailchimp_client, dry_run, with_mailchimp):
    data_api_client.find_users_iter.return_value = iter((
        {
            "id": 1234,
            "emailAddress": "walkup@walkup.eggs",
            "loggedInAt": "2000-01-12T12:22:22.00000Z",
            "personalDataRemoved": False,
        },
        {
            "id": 1235,
            "emailAddress": "alaki@abe-aku.ta",
            "loggedInAt": "2001-01-12T13:33:33.00000Z",
            "personalDataRemoved": True,
        },
        {
            "id": 1236,
            "emailAddress": "ananias@praisegod.barebones",
            "loggedInAt": "1999-02-21T02:02:02.00000Z",
            "personalDataRemoved": False,
        },
        {
            "id": 1237,
            "emailAddress": "kakachakachak@forty.warts",
            "loggedInAt": "2001-12-13T14:55:55.00000Z",
            "personalDataRemoved": True,
        },
        {
            "id": 1238,
            "emailAddress": "cotton@op.ol.is",
            "loggedInAt": "2001-12-13T14:56:56.00000Z",
            "personalDataRemoved": False,
        },
    ))
    dm_mailchimp_client.get_email_hash.side_effect = lambda email: f"hashfor({email})"
    dm_mailchimp_client.get_lists_for_email.side_effect = lambda email_address: {
        "walkup@walkup.eggs": (
            {"list_id": "kkp", "name": "Kilkenny People"},
        ),
        "alaki@abe-aku.ta": (
            {"list_id": "nw0", "name": "Northern Whig"},
            {"list_id": "kkp", "name": "Kilkenny People"},
            {"list_id": "ce", "name": "Cork Examiner"},
        ),
        "ananias@praisegod.barebones": (),
    }[email_address]

    with freeze_time("2004-06-16T13:01:02"):
        data_retention_remove_user_data(
            data_api_client=data_api_client,
            logger=mock.Mock(),
            dm_mailchimp_client=dm_mailchimp_client if with_mailchimp else None,
            dry_run=dry_run,
        )

    assert data_api_client.mock_calls == [
        mock.call.find_users_iter(),
    ] + ([] if dry_run else [
        mock.call.remove_user_personal_data(1234, "Data Retention Script 2004-06-16T13:01:02"),
        mock.call.remove_user_personal_data(1236, "Data Retention Script 2004-06-16T13:01:02"),
    ])

    if with_mailchimp:
        assert dm_mailchimp_client.get_lists_for_email.mock_calls == [
            mock.call("walkup@walkup.eggs"),
            mock.call("alaki@abe-aku.ta"),
            mock.call("ananias@praisegod.barebones"),
        ]
        assert dm_mailchimp_client.permanently_remove_email_from_list.mock_calls == ([] if dry_run else [
            mock.call(email_address="walkup@walkup.eggs", list_id="kkp"),
            mock.call(email_address="alaki@abe-aku.ta", list_id="nw0"),
            mock.call(email_address="alaki@abe-aku.ta", list_id="kkp"),
            mock.call(email_address="alaki@abe-aku.ta", list_id="ce"),
        ])
        # check there are no other stray calls on dm_mailchimp_client unaccounted for
        assert all(
            c[0] in ("get_lists_for_email", "permanently_remove_email_from_list", "get_email_hash",)
            for c in dm_mailchimp_client.mock_calls
        )
