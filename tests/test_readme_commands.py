"""tests/test_readme_commands.py: the commands table "Commands and their optional flags" cannot rot.

Why it exists (Ashley, 2026-10-30): a flag that is not written down is forgotten. The test reads every command and every
flag out of the code, with `ast` and no imports (so no Google, Postgres or click is needed to run it), and the
docs/runbook.md table out of its section, and fails when they disagree:

  * a command (a module under cdc/, bronze/, simulator/ or scripts/ with an `if __name__ == "__main__"` guard, or a `__main__.py`) has no row;
  * a row names a command that does not exist;
  * a flag in the code (`click.option("--x")` or `parser.add_argument("--x")`) is not in its row's flags column;
  * a flag in a row's flags column is not in the code;
  * a flags column that has no backticked flag does not say `none`;
  * a flag the scanner cannot read as a literal string (so the table could not be checked) is itself a problem.

Only the flags column counts: a flag mentioned in the explanation column does not satisfy the check.

The planted-fault tests build a small fake repo and prove each rule fires, so the real test passing means something."""
import ast
import re
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
COMMANDS_DOC = ROOT / "docs" / "runbook.md"
COMMAND_FOLDERS = ("cdc", "bronze", "simulator", "scripts")
SECTION = "## Commands and their optional flags"
FLAG = re.compile(r"--[a-z0-9][a-z0-9-]*")
NONE = "none"


def _has_main_guard(tree: ast.Module) -> bool:
    for node in tree.body:
        if isinstance(node, ast.If) and isinstance(node.test, ast.Compare):
            test = node.test
            if (isinstance(test.left, ast.Name) and test.left.id == "__name__" and len(test.comparators) == 1
                    and isinstance(test.comparators[0], ast.Constant) and test.comparators[0].value == "__main__"):
                return True
    return False


def _is_flag_call(node: ast.AST) -> bool:
    """click.option(...) or click.argument(...), either imported by name, or anything.add_argument(...)."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr == "add_argument" or (func.attr in ("option", "argument") and isinstance(func.value, ast.Name) and func.value.id == "click")
    return isinstance(func, ast.Name) and func.id in ("option", "argument")


def scan_flags(tree: ast.Module) -> tuple[set[str], list[str]]:
    """(the flags written as literals, the lines of flag calls that name no readable flag)."""
    flags: set[str] = set()
    unreadable: list[str] = []
    for node in ast.walk(tree):
        if not _is_flag_call(node):
            continue
        named = [a.value for a in node.args if isinstance(a, ast.Constant) and isinstance(a.value, str) and FLAG.fullmatch(a.value)]
        if named:
            flags.update(named)
        elif not any(isinstance(a, ast.Constant) and isinstance(a.value, str) and a.value.startswith("-") for a in node.args):
            unreadable.append(f"line {node.lineno}")
    return flags, unreadable


def code_commands(root: Path) -> tuple[dict[str, set[str]], list[str]]:
    """Every command under the command folders with its flags, and the problems the scan itself found."""
    commands: dict[str, set[str]] = {}
    problems: list[str] = []
    for folder in COMMAND_FOLDERS:
        for path in sorted((root / folder).rglob("*.py")):
            if path.name == "__init__.py":
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            if path.name != "__main__.py" and not _has_main_guard(tree):
                continue
            relative = path.relative_to(root)
            if folder == "scripts":
                name = relative.as_posix()
            else:
                parts = relative.with_suffix("").parts
                name = "python -m " + ".".join(parts[:-1] if path.name == "__main__.py" else parts)
            flags, unreadable = scan_flags(tree)
            commands[name] = flags
            for where in unreadable:
                problems.append(f"{name}: a flag at {where} is not a literal string, so the README table cannot be checked against it")
    return commands, problems


def readme_rows(text: str) -> tuple[Optional[dict[str, tuple[set[str], str]]], list[str]]:
    """The table rows as {command: (the flags in the flags column, that column)} (None when the section is not there
    exactly once), and the problems in the table itself."""
    problems: list[str] = []
    lines = text.splitlines()
    starts = [i for i, line in enumerate(lines) if line.strip() == SECTION]
    if len(starts) != 1:
        return None, [f"the README must have exactly one section '{SECTION}' (found {len(starts)})"]
    rows: dict[str, tuple[set[str], str]] = {}
    for line in lines[starts[0] + 1:]:
        if line.startswith("## "):
            break
        if not line.startswith("|") or re.fullmatch(r"\|[-| :]+\|", line.strip()):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 4:
            problems.append(f"a table row has {len(cells)} cells, not 4 (a stray | in a cell?): {line[:70]}")
            continue
        command = cells[0].strip("`")
        if command == "Command":
            continue
        if command in rows:
            problems.append(f"{command}: two rows")
        flags_cell = cells[2]
        found = set(re.findall(r"`(" + FLAG.pattern + r")`", flags_cell))
        rows[command] = (found, flags_cell)
    return rows, problems


def problems_between(root: Path, readme: str) -> list[str]:
    commands, problems = code_commands(root)
    rows, row_problems = readme_rows(readme)
    problems += row_problems
    if rows is None:
        return problems
    for command, flags in commands.items():
        if command not in rows:
            problems.append(f"{command}: no row in the README table")
            continue
        documented, cell = rows[command]
        for flag in sorted(flags - documented):
            problems.append(f"{command}: {flag} is in the code but not in the README flags column")
        for flag in sorted(documented - flags):
            problems.append(f"{command}: {flag} is in the README flags column but not in the code")
        if not documented and cell.strip().strip("`").lower() != NONE:
            problems.append(f"{command}: the flags column lists no flag and does not say {NONE}")
    for command in rows:
        if command not in commands:
            problems.append(f"{command}: a README row for a command that does not exist")
    return problems


# ---- the real repo -----------------------------------------------------------------------------------------------

def test_every_command_and_flag_in_the_code_is_in_the_readme_table_and_nothing_else():
    problems = problems_between(ROOT, COMMANDS_DOC.read_text(encoding="utf-8"))
    assert problems == [], "commands table and code disagree:\n" + "\n".join(problems)


def test_the_scanner_sees_the_real_flags():
    """The scanner must not go blind (a renamed decorator would make 'no flags found' look like a pass)."""
    commands, problems = code_commands(ROOT)
    assert problems == []
    assert commands["python -m bronze.main"] == {"--write", "--rebuild-catalog", "--verified-marker-file"}
    assert commands["python -m cdc.main"] == {"--once", "--follow"}
    assert commands["scripts/storage_request_counts.py"] == {"--project", "--start", "--first-end", "--end", "--planned-a"}
    assert commands["scripts/capture_pgoutput_fixtures.py"] == set()
    assert "--mode" in commands["python -m simulator.main"]
    assert len(commands) >= 16
    assert sum(len(f) for f in commands.values()) >= 55


def test_the_readme_table_parses_to_one_row_per_command():
    rows, problems = readme_rows(COMMANDS_DOC.read_text(encoding="utf-8"))
    commands, _ = code_commands(ROOT)
    assert problems == []
    assert set(rows) == set(commands)


# ---- planted faults on a fake repo ---------------------------------------------------------------------------------

CLICK_COMMAND = '''import click

@click.command()
@click.option("--alpha", is_flag=True)
@click.option("--beta", "beta_value", default=1)
def cli(alpha, beta_value):
    pass

if __name__ == "__main__":
    cli()
'''
ARGPARSE_SCRIPT = '''import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gamma", type=int, default=1)
    parser.add_argument("--delta-two")
    parser.parse_args()

if __name__ == "__main__":
    main()
'''
NO_FLAG_SCRIPT = '''def main():
    pass

if __name__ == "__main__":
    main()
'''
HEADER = "| Command | With no flag | Optional flags | When to use a flag |\n|---|---|---|---|\n"


def readme(*rows: str, section: str = SECTION) -> str:
    return f"# Title\n\n## Other\n\ntext\n\n{section}\n\nintro\n\n{HEADER}" + "\n".join(rows) + "\n\n## After\n\n| x | y |\n"


GOOD_ROWS = (
    "| `python -m cdc.thing` | does it | `--alpha`, `--beta` | when |",
    "| `scripts/tool.py` | does it | `--gamma`, `--delta-two` | when |",
    "| `scripts/plain.py` | does it | none | nothing |",
)


def fake_repo(tmp_path: Path) -> Path:
    files = {"cdc/thing.py": CLICK_COMMAND, "scripts/tool.py": ARGPARSE_SCRIPT, "scripts/plain.py": NO_FLAG_SCRIPT,
             "cdc/__init__.py": "", "cdc/library.py": "import click\n\n@click.option('--not-a-command')\ndef f():\n    pass\n"}
    for name, body in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    return tmp_path


def test_control_a_correct_fake_repo_has_no_problems(tmp_path):
    assert problems_between(fake_repo(tmp_path), readme(*GOOD_ROWS)) == []


def test_a_click_flag_missing_from_the_table_is_found(tmp_path):
    rows = (GOOD_ROWS[0].replace(", `--beta`", ""),) + GOOD_ROWS[1:]
    assert problems_between(fake_repo(tmp_path), readme(*rows)) == ["python -m cdc.thing: --beta is in the code but not in the README flags column"]


def test_an_argparse_flag_missing_from_the_table_is_found(tmp_path):
    rows = GOOD_ROWS[:1] + (GOOD_ROWS[1].replace(", `--delta-two`", ""),) + GOOD_ROWS[2:]
    assert problems_between(fake_repo(tmp_path), readme(*rows)) == ["scripts/tool.py: --delta-two is in the code but not in the README flags column"]


def test_a_flag_in_the_table_that_the_code_does_not_have_is_found(tmp_path):
    rows = (GOOD_ROWS[0].replace("`--beta`", "`--beta`, `--ghost`"),) + GOOD_ROWS[1:]
    assert problems_between(fake_repo(tmp_path), readme(*rows)) == ["python -m cdc.thing: --ghost is in the README flags column but not in the code"]


def test_a_flag_added_to_the_code_later_is_found(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/plain.py").write_text(ARGPARSE_SCRIPT.replace("--gamma", "--late"), encoding="utf-8")
    problems = problems_between(root, readme(*GOOD_ROWS))
    assert "scripts/plain.py: --late is in the code but not in the README flags column" in problems


def test_a_new_command_without_a_row_is_found(tmp_path):
    root = fake_repo(tmp_path)
    (root / "bronze").mkdir()
    (root / "bronze/newcmd.py").write_text(NO_FLAG_SCRIPT, encoding="utf-8")
    assert problems_between(root, readme(*GOOD_ROWS)) == ["python -m bronze.newcmd: no row in the README table"]


def test_a_row_for_a_command_that_does_not_exist_is_found(tmp_path):
    rows = GOOD_ROWS + ("| `scripts/gone.py` | x | none | y |",)
    assert problems_between(fake_repo(tmp_path), readme(*rows)) == ["scripts/gone.py: a README row for a command that does not exist"]


def test_a_flags_column_that_is_empty_or_vague_is_found(tmp_path):
    for cell in ("", "see below", "no flags"):
        rows = GOOD_ROWS[:2] + (f"| `scripts/plain.py` | does it | {cell} | nothing |",)
        assert problems_between(fake_repo(tmp_path), readme(*rows)) == [
            "scripts/plain.py: the flags column lists no flag and does not say none"], cell


def test_none_may_be_written_in_capitals_or_in_backticks(tmp_path):
    for cell in ("None", "NONE", "`none`"):
        rows = GOOD_ROWS[:2] + (f"| `scripts/plain.py` | does it | {cell} | nothing |",)
        assert problems_between(fake_repo(tmp_path), readme(*rows)) == [], cell


def test_a_flag_named_only_in_the_explanation_column_does_not_count(tmp_path):
    rows = ("| `python -m cdc.thing` | does it | `--alpha` | use `--beta` when needed |",) + GOOD_ROWS[1:]
    assert problems_between(fake_repo(tmp_path), readme(*rows)) == ["python -m cdc.thing: --beta is in the code but not in the README flags column"]


def test_a_flag_in_the_flags_column_without_backticks_does_not_count(tmp_path):
    rows = ("| `python -m cdc.thing` | does it | `--alpha`, --beta | when |",) + GOOD_ROWS[1:]
    assert problems_between(fake_repo(tmp_path), readme(*rows)) == ["python -m cdc.thing: --beta is in the code but not in the README flags column"]


def test_a_flag_that_is_a_prefix_of_another_does_not_count_for_it(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/tool.py").write_text(ARGPARSE_SCRIPT.replace("--delta-two", "--delta"), encoding="utf-8")
    rows = GOOD_ROWS[:1] + ("| `scripts/tool.py` | does it | `--gamma`, `--delta-two` | when |",) + GOOD_ROWS[2:]
    assert problems_between(root, readme(*rows)) == [
        "scripts/tool.py: --delta is in the code but not in the README flags column",
        "scripts/tool.py: --delta-two is in the README flags column but not in the code"]


def test_a_short_flag_is_not_asked_for_and_a_module_without_a_main_guard_is_not_a_command(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/plain.py").write_text('import click\n\n@click.option("-v", "--verbose")\ndef f():\n    pass\n\nif __name__ == "__main__":\n    f()\n', encoding="utf-8")
    rows = GOOD_ROWS[:2] + ("| `scripts/plain.py` | does it | `--verbose` | nothing |",)
    assert problems_between(root, readme(*rows)) == []


def test_a_flag_the_scanner_cannot_read_is_a_problem_not_a_pass(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/plain.py").write_text('import click\nNAMES = ["--x"]\n\n@click.option(*NAMES)\ndef f():\n    pass\n\nif __name__ == "__main__":\n    f()\n', encoding="utf-8")
    problems = problems_between(root, readme(*GOOD_ROWS))
    assert problems == ["scripts/plain.py: a flag at line 4 is not a literal string, so the README table cannot be checked against it"]


def test_a_positional_argument_is_a_problem_too(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/plain.py").write_text('import argparse\np = argparse.ArgumentParser()\np.add_argument("name")\n\nif __name__ == "__main__":\n    p.parse_args()\n', encoding="utf-8")
    problems = problems_between(root, readme(*GOOD_ROWS))
    assert problems == ["scripts/plain.py: a flag at line 3 is not a literal string, so the README table cannot be checked against it"]


def test_a_missing_or_doubled_section_is_a_problem(tmp_path):
    root = fake_repo(tmp_path)
    assert len(problems_between(root, readme(*GOOD_ROWS, section="## Something else"))) == 1
    doubled = readme(*GOOD_ROWS) + f"\n{SECTION}\n"
    assert any("exactly one section" in p for p in problems_between(root, doubled))


def test_a_row_with_a_stray_bar_is_a_problem(tmp_path):
    rows = ("| `python -m cdc.thing` | does it | `--alpha`, `--beta` | a | b |",) + GOOD_ROWS[1:]
    problems = problems_between(fake_repo(tmp_path), readme(*rows))
    assert any("5 cells" in p for p in problems) and "python -m cdc.thing: no row in the README table" in problems


def test_two_rows_for_one_command_are_a_problem(tmp_path):
    problems = problems_between(fake_repo(tmp_path), readme(*GOOD_ROWS, GOOD_ROWS[2]))
    assert problems == ["scripts/plain.py: two rows"]


def test_rows_after_the_section_are_ignored(tmp_path):
    text = readme(*GOOD_ROWS) + "| `scripts/other.py` | x | `--zzz` | y |\n"
    assert problems_between(fake_repo(tmp_path), text) == []


def test_a_bare_option_call_from_click_import_is_read(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/plain.py").write_text('from click import option\n\n@option("--bare")\ndef f():\n    pass\n\nif __name__ == "__main__":\n    f()\n', encoding="utf-8")
    assert "scripts/plain.py: --bare is in the code but not in the README flags column" in problems_between(root, readme(*GOOD_ROWS))


def test_an_option_call_on_something_that_is_not_click_is_not_a_flag(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/plain.py").write_text('config = object()\nconfig.option("--not-a-flag")\n\nif __name__ == "__main__":\n    pass\n', encoding="utf-8")
    assert problems_between(root, readme(*GOOD_ROWS)) == []


def test_flags_with_digits_and_aliases_are_all_read(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/plain.py").write_text('import click\n\n@click.option("--level2", "--also-long", "short")\ndef f(short):\n    pass\n\nif __name__ == "__main__":\n    f()\n', encoding="utf-8")
    problems = problems_between(root, readme(*GOOD_ROWS))
    assert problems == ["scripts/plain.py: --also-long is in the code but not in the README flags column",
                        "scripts/plain.py: --level2 is in the code but not in the README flags column"]


def test_only_a_comparison_of_name_with_main_is_a_guard(tmp_path):
    root = fake_repo(tmp_path)
    (root / "cdc/library.py").write_text('import click\n\n@click.option("--lib")\ndef f():\n    pass\n\nmode = 1\nif mode == "__main__":\n    pass\nif __name__ == "other":\n    pass\n', encoding="utf-8")
    assert problems_between(root, readme(*GOOD_ROWS)) == []


def test_a_guard_that_is_not_the_last_statement_still_counts(tmp_path):
    root = fake_repo(tmp_path)
    (root / "cdc/later.py").write_text('import click\n\n@click.option("--late-flag")\ndef f():\n    pass\n\nif __name__ == "__main__":\n    f()\n\nTAIL = 1\n', encoding="utf-8")
    assert problems_between(root, readme(*GOOD_ROWS)) == ["python -m cdc.later: no row in the README table"]


def test_an_init_file_is_never_a_command_and_a_main_file_always_is(tmp_path):
    root = fake_repo(tmp_path)
    (root / "cdc/__init__.py").write_text('import click\n\n@click.option("--in-init")\ndef f():\n    pass\n\nif __name__ == "__main__":\n    f()\n', encoding="utf-8")
    (root / "cdc/__main__.py").write_text('import click\n\n@click.option("--in-main")\ndef f():\n    pass\n', encoding="utf-8")
    assert problems_between(root, readme(*GOOD_ROWS)) == ["python -m cdc: no row in the README table"]
    rows = GOOD_ROWS + ("| `python -m cdc` | x | `--in-main` | y |",)
    assert problems_between(root, readme(*rows)) == []


def test_commands_in_sub_folders_are_found_and_named_by_their_path(tmp_path):
    root = fake_repo(tmp_path)
    (root / "cdc/sub").mkdir()
    (root / "cdc/sub/deep.py").write_text(NO_FLAG_SCRIPT, encoding="utf-8")
    (root / "scripts/sub").mkdir()
    (root / "scripts/sub/deeper.py").write_text(NO_FLAG_SCRIPT, encoding="utf-8")
    assert problems_between(root, readme(*GOOD_ROWS)) == ["python -m cdc.sub.deep: no row in the README table",
                                                          "scripts/sub/deeper.py: no row in the README table"]


def test_none_must_be_the_whole_cell(tmp_path):
    rows = GOOD_ROWS[:2] + ("| `scripts/plain.py` | does it | none of them | nothing |",)
    assert problems_between(fake_repo(tmp_path), readme(*rows)) == ["scripts/plain.py: the flags column lists no flag and does not say none"]


def test_a_click_argument_is_a_problem_too(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/plain.py").write_text('import click\n\n@click.argument("name")\ndef f(name):\n    pass\n\nif __name__ == "__main__":\n    f()\n', encoding="utf-8")
    assert problems_between(root, readme(*GOOD_ROWS)) == [
        "scripts/plain.py: a flag at line 3 is not a literal string, so the README table cannot be checked against it"]
    (root / "scripts/plain.py").write_text('from click import argument\n\n@argument("name")\ndef f(name):\n    pass\n\nif __name__ == "__main__":\n    f()\n', encoding="utf-8")
    assert len(problems_between(root, readme(*GOOD_ROWS))) == 1


# ---------------------------------------------------------------------------
# exit codes
# ---------------------------------------------------------------------------

def test_the_bronze_exit_codes_are_the_documented_values():
    """The README documents 0 / 1 / 2 / 3 / 130 for every Bronze command. Pin them so a change is visible."""
    from bronze.check import EXIT_OK, EXIT_ERROR, EXIT_CONTRACT, EXIT_BUCKET, EXIT_INTERRUPTED
    assert EXIT_OK == 0
    assert EXIT_ERROR == 1
    assert EXIT_CONTRACT == 2
    assert EXIT_BUCKET == 3
    assert EXIT_INTERRUPTED == 130
