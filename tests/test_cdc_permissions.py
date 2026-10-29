"""
Tests for cdc/permissions.py: the least-privilege check for the cdc-consumer account.

The client is a strict fake: a bucket answers test_iam_permissions and nothing else, so any call that
could write, read or list an object (blob, list_blobs, upload) raises and fails the test. That is the
"no write, no object call" promise, checked.
"""
from __future__ import annotations

import pytest
from click.testing import CliRunner

import cdc.permissions as perm
from cdc.permissions import (ACCOUNT, EXIT_DIFFERS, EXIT_OK, EXIT_REFUSED, LANDING_GRANTED, NEVER, Expectation,
                             Row, ask, expectations, run)

LANDING, BRONZE = "cdc_landing", "fintech_bronze"
GOOD = {LANDING: set(LANDING_GRANTED), BRONZE: set()}


class StrictBucket:
    def __init__(self, name: str, granted: set[str], log: list) -> None:
        self.name, self._granted, self._log = name, granted, log

    def test_iam_permissions(self, permissions):
        self._log.append((self.name, list(permissions)))
        return [p for p in permissions if p in self._granted]


class StrictClient:
    """Only .bucket(name).test_iam_permissions exists."""

    def __init__(self, granted: dict[str, set[str]]) -> None:
        self._granted, self.calls, self.buckets_made = granted, [], []

    def bucket(self, name: str) -> StrictBucket:
        self.buckets_made.append(name)
        return StrictBucket(name, self._granted.get(name, set()), self.calls)


def go(granted=GOOD, identity=ACCOUNT, wanted=None):
    lines: list[str] = []
    client = StrictClient({k: set(v) for k, v in granted.items()})
    code = run(client, identity, expectations(LANDING, BRONZE) if wanted is None else wanted, out=lines.append)
    return code, "\n".join(lines), client


def test_the_account_and_project_are_the_real_ones():
    assert ACCOUNT == "cdc-consumer@payments-512910.iam.gserviceaccount.com"
    from cdc.config import settings
    assert settings.project == "payments-512910"
    assert (EXIT_OK, EXIT_DIFFERS, EXIT_REFUSED) == (0, 1, 2)


def test_the_consumer_may_create_get_and_list_and_nothing_else_on_the_landing_bucket():
    assert set(LANDING_GRANTED) == {"storage.objects.create", "storage.objects.get", "storage.objects.list"}
    landing = expectations(LANDING, BRONZE)[0]
    assert landing.bucket == LANDING and set(landing.granted) == set(LANDING_GRANTED)
    assert set(landing.denied) == set(NEVER)


@pytest.mark.parametrize("permission", [
    "storage.objects.update", "storage.objects.delete", "storage.buckets.get",
    "storage.buckets.update", "storage.buckets.delete", "storage.buckets.setIamPolicy"])
def test_these_are_never_granted(permission):
    assert permission in NEVER
    assert permission in expectations(LANDING, BRONZE)[0].denied
    assert permission in expectations(LANDING, BRONZE)[1].denied


@pytest.mark.parametrize("permission", ["storage.objects.getIamPolicy", "storage.objects.setIamPolicy"])
def test_object_level_iam_permissions_are_never_asked_because_google_rejects_them_on_uniform_access_buckets(permission):
    # The first real run got 400 "Cannot test storage.objects.getIamPolicy or storage.objects.setIamPolicy on buckets
    # with uniform bucket-level access enabled" for the whole call. A fake accepts any name, so this pins the rule.
    assert permission in perm.NOT_TESTABLE
    assert permission not in NEVER and permission not in LANDING_GRANTED
    for expectation in expectations(LANDING, BRONZE):
        assert permission not in expectation.granted and permission not in expectation.denied
    _, _, client = go()
    for _, asked in client.calls:
        assert permission not in asked


def test_a_fake_that_behaves_like_google_on_uniform_access_buckets_accepts_every_call_made():
    class Rejecting(StrictBucket):
        def test_iam_permissions(self, permissions):
            bad = [p for p in permissions if p in perm.NOT_TESTABLE]
            if bad:
                raise ValueError(f"400 Cannot test {bad[0]} on buckets with uniform bucket-level access enabled")
            return super().test_iam_permissions(permissions)

    class RejectingClient(StrictClient):
        def bucket(self, name):
            return Rejecting(name, self._granted.get(name, set()), self.calls)

    lines: list[str] = []
    code = run(RejectingClient({k: set(v) for k, v in GOOD.items()}), ACCOUNT, expectations(LANDING, BRONZE), out=lines.append)
    assert code == EXIT_OK, lines


def test_the_consumer_has_nothing_at_all_on_the_bronze_bucket():
    bronze = expectations(LANDING, BRONZE)[1]
    assert bronze.bucket == BRONZE and bronze.granted == ()
    assert set(bronze.denied) == set(LANDING_GRANTED) | set(NEVER)


def test_expected_state_passes_with_exit_zero_and_says_so():
    code, text, _ = go()
    assert code == EXIT_OK
    assert f"account {ACCOUNT}" in text and "gs://cdc_landing" in text and "gs://fintech_bronze" in text
    assert "every permission is as the least-privilege table says" in text
    assert "WRONG" not in text
    assert "granted  storage.objects.create  ok" in text
    assert "denied   storage.objects.delete  ok" in text


def test_each_bucket_is_asked_once_for_every_permission_granted_or_denied():
    _, _, client = go()
    assert [name for name, _ in client.calls] == [LANDING, BRONZE]
    landing, bronze = client.calls[0][1], client.calls[1][1]
    assert sorted(landing) == sorted(LANDING_GRANTED + NEVER)
    assert sorted(bronze) == sorted(LANDING_GRANTED + NEVER)


def test_only_the_permission_test_is_ever_called():
    _, _, client = go()
    assert client.buckets_made == [LANDING, BRONZE]  # a strict fake: .blob or .list_blobs would have raised


@pytest.mark.parametrize("extra", ["storage.objects.update", "storage.objects.delete", "storage.buckets.get",
                                   "storage.buckets.setIamPolicy"])
def test_a_grant_that_should_not_be_there_on_the_landing_bucket_fails_and_names_it(extra):
    code, text, _ = go({LANDING: set(LANDING_GRANTED) | {extra}, BRONZE: set()})
    assert code == EXIT_DIFFERS
    assert f"granted  {extra}  WRONG: this should be denied" in text
    assert "1 permission(s) differ" in text


@pytest.mark.parametrize("missing", list(LANDING_GRANTED))
def test_a_missing_grant_on_the_landing_bucket_fails_and_names_it(missing):
    code, text, _ = go({LANDING: set(LANDING_GRANTED) - {missing}, BRONZE: set()})
    assert code == EXIT_DIFFERS
    assert f"denied   {missing}  WRONG: this should be granted" in text


@pytest.mark.parametrize("extra", ["storage.objects.get", "storage.objects.create", "storage.buckets.get"])
def test_any_grant_on_the_bronze_bucket_fails(extra):
    code, text, _ = go({LANDING: set(LANDING_GRANTED), BRONZE: {extra}})
    assert code == EXIT_DIFFERS and f"granted  {extra}  WRONG" in text


def test_every_difference_is_counted():
    code, text, _ = go({LANDING: (set(LANDING_GRANTED) | {"storage.objects.delete"}) - {"storage.objects.get"},
                        BRONZE: {"storage.objects.list"}})
    assert code == EXIT_DIFFERS and "3 permission(s) differ" in text


def test_the_owner_would_fail_loudly_if_it_got_this_far():
    everything = set(LANDING_GRANTED) | set(NEVER)
    code, text, _ = go({LANDING: everything, BRONZE: everything})
    assert code == EXIT_DIFFERS and text.count("WRONG: this should be denied") == len(NEVER) + len(NEVER) + len(LANDING_GRANTED)


@pytest.mark.parametrize("identity", ["", "ashley.esau@gmail.com",
                                      "bronze-writer@payments-512910.iam.gserviceaccount.com",
                                      "cdc-consumer@payments-512910.iam.gserviceaccount.com ",
                                      "CDC-consumer@payments-512910.iam.gserviceaccount.com"])
def test_any_other_identity_is_refused_before_a_single_bucket_call(identity):
    code, text, client = go(identity=identity)
    assert code == EXIT_REFUSED
    assert "Refusing to go on" in text and ACCOUNT in text
    assert client.calls == [] and client.buckets_made == []


def test_a_user_login_is_described_as_one():
    _, text, _ = go(identity="")
    assert "a user login" in text


def test_ask_returns_one_row_per_permission_with_expected_and_actual():
    client = StrictClient({LANDING: {"storage.objects.create", "storage.objects.delete"}})
    rows = ask(client, Expectation(LANDING, ("storage.objects.create", "storage.objects.get"), ("storage.objects.delete",)))
    assert rows == [Row(LANDING, "storage.objects.create", True, True), Row(LANDING, "storage.objects.get", True, False),
                    Row(LANDING, "storage.objects.delete", False, True)]
    assert [r.ok for r in rows] == [True, False, False]


# The command line, with the two Google calls replaced.

@pytest.fixture
def cli(monkeypatch):
    made: list[str] = []
    state = {"identity": ACCOUNT, "granted": GOOD, "ask_fails": False}

    def fake_identity():
        if state["ask_fails"]:
            raise RuntimeError("token service unreachable")
        return state["identity"]

    def fake_client(project):
        made.append(project)
        return StrictClient({k: set(v) for k, v in state["granted"].items()})

    monkeypatch.setattr(perm, "resolve_identity", fake_identity)
    monkeypatch.setattr(perm, "make_client", fake_client)
    return state, made


def test_command_passes_with_exit_zero_and_builds_the_client_for_the_project(cli):
    state, made = cli
    result = CliRunner().invoke(perm.main, [])
    assert result.exit_code == EXIT_OK, result.output
    assert made == ["payments-512910"]
    assert "gs://cdc_landing" in result.output and "gs://fintech_bronze" in result.output


def test_command_takes_the_project_and_bucket_names_from_its_options(cli):
    state, made = cli
    state["granted"] = {"my_landing": set(LANDING_GRANTED), "my_bronze": set()}
    result = CliRunner().invoke(perm.main, ["--project", "other-project", "--landing-bucket", "my_landing",
                                            "--bronze-bucket", "my_bronze"])
    assert result.exit_code == EXIT_OK, result.output
    assert made == ["other-project"] and "gs://my_landing" in result.output and "gs://my_bronze" in result.output


def test_command_exits_one_on_a_difference(cli):
    state, _ = cli
    state["granted"] = {LANDING: set(LANDING_GRANTED) | {"storage.objects.delete"}, BRONZE: set()}
    result = CliRunner().invoke(perm.main, [])
    assert result.exit_code == EXIT_DIFFERS and "storage.objects.delete" in result.output


def test_command_with_the_wrong_account_exits_two_and_never_makes_a_storage_client(cli):
    state, made = cli
    state["identity"] = "ashley.esau@gmail.com"
    result = CliRunner().invoke(perm.main, [])
    assert result.exit_code == EXIT_REFUSED and "Refusing to go on" in result.output
    assert made == []


def test_command_exits_two_when_google_cannot_be_asked_and_prints_no_traceback(cli):
    state, made = cli
    state["ask_fails"] = True
    result = CliRunner().invoke(perm.main, [])
    assert result.exit_code == EXIT_REFUSED
    assert "Could not ask Google: RuntimeError: token service unreachable" in result.output
    assert "Traceback" not in result.output and made == []


def test_default_landing_bucket_is_the_consumers_own_setting():
    from cdc.config import settings
    option = {p.name: p for p in perm.main.params}["landing_bucket"]
    assert option.default == settings.gcs_bucket
    assert {p.name: p for p in perm.main.params}["bronze_bucket"].default == "fintech_bronze"
