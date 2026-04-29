import argparse
import logging
import subprocess
import sys
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_step(name: str, command: list[str]) -> None:
    logger.info("==================================================")
    logger.info("Iniciando paso: %s", name)
    logger.info("Comando: %s", " ".join(command))
    logger.info("==================================================")

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Falló el paso: {name}")

    logger.info("Paso finalizado correctamente: %s", name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--enrich-limit", type=int, default=50)
    parser.add_argument("--drive-limit", type=int, default=50)
    parser.add_argument("--text-limit", type=int, default=50)
    parser.add_argument("--structured-limit", type=int, default=50)
    parser.add_argument("--skip-monitor", action="store_true")
    parser.add_argument("--skip-enrich", action="store_true")
    parser.add_argument("--skip-drive", action="store_true")
    parser.add_argument("--skip-text-extract", action="store_true")
    parser.add_argument("--skip-structured-extract", action="store_true")
    parser.add_argument("--drive-dry-run", action="store_true")
    args = parser.parse_args()

    python = sys.executable

    logger.info("Iniciando pipeline completo RegAlert DIGEMID")

    if not args.skip_monitor:
        run_step(
            "Monitorear DIGEMID y registrar alertas",
            [python, "main.py"],
        )

    if not args.skip_enrich:
        run_step(
            "Enriquecer metadata de alertas",
            [
                python,
                "scripts/enrich_month_alertas.py",
                "--limit",
                str(args.enrich_limit),
            ],
        )

    if not args.skip_drive:
        drive_command = [
            python,
            "scripts/upload_pdfs_to_drive.py",
            "--limit",
            str(args.drive_limit),
        ]

        if args.drive_dry_run:
            drive_command.append("--dry-run")

        run_step(
            "Subir PDFs pendientes a Google Drive",
            drive_command,
        )

    if not args.skip_text_extract:
        run_step(
            "Extraer texto basico de PDFs a Supabase",
            [
                python,
                "scripts/extract_pdf_text_to_supabase.py",
                "--limit",
                str(args.text_limit),
            ],
        )

    if not args.skip_structured_extract:
        run_step(
            "Extraer productos estructurados de alertas DIGEMID",
            [
                python,
                "scripts/extract_alerta_productos.py",
                "--limit",
                str(args.structured_limit),
            ],
        )

    logger.info("Pipeline completo finalizado correctamente")


if __name__ == "__main__":
    main()
