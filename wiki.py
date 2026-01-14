"""
Client MediaWiki et utilitaires de parsing wikitext.
"""

import re
import time
import logging
from typing import Optional, Dict, List
from urllib.parse import quote

import requests
import mwparserfromhell

from config import TOLKIEN_GATEWAY_API, HTTP_HEADERS, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

REQUEST_DELAY = 1.0  # 1 seconde entre chaque requete
MAX_RETRIES = 3


def clean_wikitext(text: str) -> str:
    if not text:
        return ""
    try:
        wikicode = mwparserfromhell.parse(text)
        plain_text = wikicode.strip_code()
    except Exception:
        plain_text = text
    plain_text = re.sub(r'\[\d+\]', '', plain_text)
    plain_text = re.sub(r'<[^>]+>', '', plain_text)
    plain_text = re.sub(r'\s+', ' ', plain_text).strip()
    return plain_text.strip('"\'')


def clean_entity_name(name: str) -> str:
    if not name:
        return ""
    name = name.replace('(', '_').replace(')', '_')
    name = name.replace("'", "").replace("'", "")
    name = name.replace(' ', '_')
    name = re.sub(r'[^\w\-_]', '', name)
    name = re.sub(r'_+', '_', name)
    return name.strip('_')


def extract_internal_links(text: str) -> List[str]:
    if not text:
        return []
    try:
        wikicode = mwparserfromhell.parse(text)
        links = wikicode.filter_wikilinks()
    except Exception:
        return []
    result = []
    excluded = ['category:', 'file:', 'image:', 'template:', 'wikipedia:', 'help:', 'special:', 'talk:', 'user:', 'portal:']
    for link in links:
        title = str(link.title).strip()
        if any(title.lower().startswith(p) for p in excluded) or '#' in title:
            continue
        result.append(title)
    return result


def split_on_br(text: str) -> List[str]:
    if not text:
        return []
    text = re.sub(r'<br\s*/?>', '|||', text, flags=re.IGNORECASE)
    return [p.strip() for p in text.split('|||') if p.strip()]


def clean_date_field(text: str) -> str:
    if not text:
        return ""
    try:
        wikicode = mwparserfromhell.parse(text)
        for template in wikicode.filter_templates():
            tname = str(template.name).strip()
            if tname in ['FA', 'SA', 'TA', 'FoA', 'YT', 'YS', 'VY'] and template.params:
                year = str(template.params[0].value).strip()
                year = re.sub(r'\[.*?\]', '', year)
                year = re.sub(r'<ref[^>]*>.*?</ref>', '', year, flags=re.DOTALL)
                return f"{tname} {year}".strip()
        plain = wikicode.strip_code()
        plain = re.sub(r'<ref[^>]*>.*?</ref>', '', plain, flags=re.DOTALL)
        return re.sub(r'\[.*?\]', '', plain).strip()
    except Exception:
        return text.strip()


def is_valid_date(date_str: str) -> bool:
    if not date_str:
        return False
    invalid = [r'^[,.\s]*$', r'^c\.$', r'^around$', r'^unknown$', r'^late.*age$', r'^early.*age$']
    for pattern in invalid:
        if re.match(pattern, date_str.lower().strip(), re.IGNORECASE):
            return False
    return True


def build_image_url(filename: str) -> str:
    """
    Construit l'URL vers l'image sur Tolkien Gateway.
    Retourne l'URL de la page File: (pas l'image directe).
    """
    if not filename:
        return None
    # Nettoyer le nom de fichier
    filename = filename.strip()
    # Enlever les préfixes File: ou Image: s'ils sont présents
    for prefix in ['File:', 'Image:', 'file:', 'image:']:
        if filename.startswith(prefix):
            filename = filename[len(prefix):]
    return f"https://tolkiengateway.net/wiki/File:{quote(filename.strip())}"


def get_image_direct_url(filename: str, wiki_client=None) -> str:
    """
    Récupère l'URL directe de l'image depuis l'API MediaWiki.
    """
    if not filename:
        return None
    
    # Nettoyer le nom de fichier
    filename = filename.strip()
    for prefix in ['File:', 'Image:', 'file:', 'image:']:
        if filename.startswith(prefix):
            filename = filename[len(prefix):]
    
    try:
        if wiki_client is None:
            wiki_client = WikiClient()
        
        params = {
            "action": "query",
            "titles": f"File:{filename}",
            "prop": "imageinfo",
            "iiprop": "url",
            "format": "json"
        }
        
        data = wiki_client._request(params)
        if data and "query" in data:
            pages = data["query"].get("pages", {})
            for page_id, page_data in pages.items():
                if page_id != "-1":
                    imageinfo = page_data.get("imageinfo", [])
                    if imageinfo:
                        return imageinfo[0].get("url")
    except Exception as e:
        logger.debug(f"Error getting image URL for {filename}: {e}")
    
    # construire une URL probable basée sur le hash MD5
    # Format MediaWiki: /images/a/ab/filename
    import hashlib
    md5 = hashlib.md5(filename.encode('utf-8')).hexdigest()
    return f"https://tolkiengateway.net/w/images/{md5[0]}/{md5[:2]}/{quote(filename)}"


class WikiClient:
    def __init__(self, api_url: str = TOLKIEN_GATEWAY_API):
        self.api_url = api_url
        self.session = requests.Session()
        self.session.headers.update(HTTP_HEADERS)
        self.last_request = 0

    def _request(self, params: Dict) -> Optional[Dict]:
        elapsed = time.time() - self.last_request
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self.last_request = time.time()
        params["format"] = "json"
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(self.api_url, params=params, timeout=REQUEST_TIMEOUT)
                
                if response.status_code == 429:
                    wait_time = (attempt + 1) * 5  # 5s, 10s, 15s
                    logger.warning(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                    continue
                logger.error(f"API error: {e}")
                return None
        return None

    def get_page_wikitext(self, title: str) -> Optional[str]:
        data = self._request({"action": "parse", "page": title, "prop": "wikitext"})
        if data and "parse" in data:
            return data["parse"]["wikitext"]["*"]
        return None

    def get_category_members(self, category: str, limit: int = 500) -> List[str]:
        members = []
        cont = None
        while len(members) < limit:
            params = {
                "action": "query",
                "list": "categorymembers",
                "cmtitle": f"Category:{category}",
                "cmlimit": min(500, limit - len(members)),
                "cmnamespace": 0,
            }
            if cont:
                params.update(cont)
            data = self._request(params)
            if not data or "query" not in data:
                break
            members.extend([m["title"] for m in data["query"].get("categorymembers", [])])
            if "continue" not in data:
                break
            cont = data["continue"]
        return members[:limit]

    def get_external_links(self, title: str) -> List[str]:
        data = self._request({"action": "parse", "page": title, "prop": "externallinks"})
        if data and "parse" in data:
            return data["parse"].get("externallinks", [])
        return []

    def search(self, query: str, limit: int = 10) -> List[Dict]:
        data = self._request({"action": "query", "list": "search", "srsearch": query, "srlimit": limit})
        if data and "query" in data:
            return data["query"].get("search", [])
        return []
