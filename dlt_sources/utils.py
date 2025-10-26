import requests
from pathlib import Path

def download_file_if_not_exists(url: str, dest_path: Path):
    """Descarga un archivo desde una URL si no existe en el destino."""
    if dest_path.exists():
        print(f"Archivo '{dest_path}' ya existe. Saltando descarga.")
        return

    print(f"Descargando archivo desde {url} a '{dest_path}'...")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print("Descarga completada.")
