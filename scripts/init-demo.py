"""Create local Compose credentials and input without putting secrets in git."""

import secrets
from pathlib import Path

if not Path(".env").exists():
    Path(".env").write_text(
        "\n".join(
            f"{name}={secrets.token_hex(24)}"
            for name in ("DATAFLOW_DB_PASSWORD", "DATAFLOW_ADMIN_TOKEN", "DATAFLOW_WORKER_TOKEN")
        )
        + "\n"
    )
    Path(".env").chmod(0o600)
root = Path("data")
root.mkdir(exist_ok=True)
# Local bind mount only. Production uses a PVC owned by the worker service UID.
root.chmod(0o777)
(root / "input.txt").write_text("hello world hello\n")
print("Ready: docker compose up --build -d --scale worker=2")
