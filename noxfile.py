import nox


@nox.session(python=False)
def format_and_test(session):
    session.run("poetry", "install", "--with", "dev")
    session.run("poetry", "run", "ruff", "format", "ogr")
    session.run("poetry", "run", "ruff", "check", "--fix", "ogr")


@nox.session(python=False)
def make_docs(session):
    session.run("poetry", "install", "--with", "dev")
    session.run(
        "poetry",
        "run",
        "pdoc",
        "-d",
        "restructuredtext",
        "-o",
        "doc",
        "--math",
        "--mermaid",
        "ogr",
    )
