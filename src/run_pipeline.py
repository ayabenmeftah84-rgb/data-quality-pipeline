"""Lance tout le pipeline en une commande : profilage -> nettoyage -> validation.

Usage :
    python src/run_pipeline.py                        # toutes les etapes
    python src/run_pipeline.py --steps clean validate # seulement certaines etapes
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Ordre d'execution : chaque etape est un script de src/
STEPS = {
    "profile": ("Profilage des donnees brutes", "src/profile_data.py"),
    "clean": ("Nettoyage et quarantaine", "src/clean_data.py"),
    "validate": ("Validation Great Expectations avant/apres", "src/validate_data.py"),
}
RAW_FILE = ROOT / "data/raw/listings.csv.gz"
CLEAN_DIR = ROOT / "data/clean/listings_clean.parquet"


def build_plan(requested=None):
    """Renvoie la liste des etapes a lancer, toujours dans l'ordre du pipeline."""
    requested = list(requested) if requested else list(STEPS)
    unknown = [s for s in requested if s not in STEPS]
    if unknown:
        raise ValueError(f"Etape(s) inconnue(s) : {', '.join(unknown)}. Etapes possibles : {', '.join(STEPS)}")
    return [s for s in STEPS if s in requested]


def check_inputs(plan):
    """Renvoie un message d'erreur si un fichier necessaire manque, sinon None."""
    if not RAW_FILE.exists():
        return f"Fichier de donnees introuvable : {RAW_FILE} (voir la section Dataset du README)."
    if "validate" in plan and "clean" not in plan and not CLEAN_DIR.exists():
        return "La validation a besoin des donnees nettoyees : lancez aussi l'etape 'clean'."
    return None


def main(argv=None):
    parser = argparse.ArgumentParser(description="Lance le pipeline de qualite des donnees.")
    parser.add_argument("--steps", nargs="+", metavar="STEP", help=f"etapes a lancer parmi : {', '.join(STEPS)}")
    args = parser.parse_args(argv)

    try:
        plan = build_plan(args.steps)
    except ValueError as error:
        print(error)
        return 2
    problem = check_inputs(plan)
    if problem:
        print(problem)
        return 2

    durations = {}
    for i, step in enumerate(plan, start=1):
        title, script = STEPS[step]
        print(f"\n===== [{i}/{len(plan)}] {title} ({script}) =====", flush=True)
        start = time.time()
        result = subprocess.run([sys.executable, script], cwd=ROOT)
        durations[step] = time.time() - start
        if result.returncode != 0:
            print(f"\nECHEC a l'etape '{step}' (code {result.returncode}). Pipeline arrete.")
            return result.returncode

    print("\n===== Pipeline termine =====")
    for step, seconds in durations.items():
        print(f"  {step:<10} {seconds:6.0f} s")
    print("Resultats : reports/ (profile_before.csv, cleaning_log.json, validation_report.md)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
