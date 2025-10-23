from __future__ import annotations

import io
from typing import Dict, List, Optional, Tuple

from googleapiclient.http import MediaIoBaseDownload

from .clients import build_services

# ─────────────────────────────────────────────────────────────────────────────
# DRIVE вспомогательные функции


def ensure_subfolder(drive, parent_id: str, name: str) -> str:
    # В Drive query одинарные кавычки внутри имени надо экранировать обратным слешем
    safe_name = name.replace("'", "\\'")
    query = (
        "mimeType='application/vnd.google-apps.folder' and trashed=false "
        f"and '{parent_id}' in parents and name='{safe_name}'"
    )
    resp = (
        drive.files()
        .list(
            q=query,
            fields="files(id, name)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora="allDrives",
        )
        .execute()
    )
    files = resp.get("files", [])
    if files:
        return files[0]["id"]

    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = (
        drive.files()
        .create(
            body=meta,
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return folder["id"]


def get_file_mime_type(drive, file_id: str) -> str:
    info = (
        drive.files()
        .get(
            fileId=file_id,
            fields="id, mimeType",
            supportsAllDrives=True,
        )
        .execute()
    )
    return info.get("mimeType", "")


def resolve_shortcut_target(drive, file_id: str) -> str:
    """
    Если file_id указывает на ярлык (shortcut), возвращает targetId,
    иначе возвращает исходный file_id.
    """
    info = (
        drive.files()
        .get(
            fileId=file_id,
            fields="id, mimeType, shortcutDetails(targetId)",
            supportsAllDrives=True,
        )
        .execute()
    )
    if info.get("mimeType") == "application/vnd.google-apps.shortcut":
        target = (info.get("shortcutDetails") or {}).get("targetId")
        if target:
            return target
    return file_id


def copy_slides_to_folder(
    drive, template_id: str, title: str, parent_folder_id: str
) -> str:
    # 1) Если это ярлык — резолвим реальный файл
    real_id = resolve_shortcut_target(drive, template_id)

    # Узнаём тип исходника
    src_mime = get_file_mime_type(drive, real_id)

    body = {"name": title, "parents": [parent_folder_id]}

    # 2) Если исходник — уже Google Slides, обычная копия
    if src_mime == "application/vnd.google-apps.presentation":
        copied = (
            drive.files()
            .copy(
                fileId=real_id,
                body=body,
                fields="id, mimeType",
                supportsAllDrives=True,
            )
            .execute()
        )
        file_id = copied["id"]
        mime = copied.get("mimeType") or get_file_mime_type(drive, file_id)
        if mime != "application/vnd.google-apps.presentation":
            raise RuntimeError(
                f"Copied file is not a Google Slides presentation (mimeType={mime}). "
                f"Check template permissions and type."
            )
        return file_id

    # 3) Если исходник, например, PPTX — пытаемся сразу скопировать с конверсией в Slides
    if (
        src_mime
        == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    ):
        copied = (
            drive.files()
            .copy(
                fileId=real_id,
                body={**body, "mimeType": "application/vnd.google-apps.presentation"},
                fields="id, mimeType",
                supportsAllDrives=True,
            )
            .execute()
        )
        file_id = copied["id"]
        mime = copied.get("mimeType") or get_file_mime_type(drive, file_id)
        if mime != "application/vnd.google-apps.presentation":
            raise RuntimeError(
                f"Copy-with-conversion failed (mimeType={mime}). "
                f"Open the template in Google Slides and use 'File → Save as Google Slides', then use that new ID."
            )
        return file_id

    # 4) Другой тип — даём понятную ошибку
    raise RuntimeError(
        f"Template file is not a Google Slides nor PPTX (mimeType={src_mime}). "
        f"Open it with Google Slides and save as Google Slides, then use that ID."
    )


def export_slides_to_pdf(drive, presentation_id: str) -> bytes:
    """
    Экспортирует презентацию (Google Slides) в PDF и возвращает байты.
    """
    request = drive.files().export(fileId=presentation_id, mimeType="application/pdf")
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        # можно логировать status.progress() при желании
    return fh.getvalue()


def delete_file(drive, file_id: str) -> None:
    drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()


# ─────────────────────────────────────────────────────────────────────────────
# SLIDES вспомогательные функции


def get_presentation_page_ids(slides, presentation_id: str) -> List[str]:
    """
    Возвращает список pageObjectId всех слайдов презентации (в порядке).
    """
    pres = slides.presentations().get(presentationId=presentation_id).execute()
    pages = pres.get("slides", [])
    return [p.get("objectId") for p in pages]


def duplicate_slide(slides, presentation_id: str, page_object_id: str) -> str:
    """
    Дублирует указанный слайд. Возвращает objectId созданного слайда.
    """
    req = {"duplicateObject": {"objectId": page_object_id}}
    resp = (
        slides.presentations()
        .batchUpdate(presentationId=presentation_id, body={"requests": [req]})
        .execute()
    )

    replies = resp.get("replies", [])
    if not replies:
        raise RuntimeError("Failed to duplicate slide")
    return replies[0]["duplicateObject"]["objectId"]


def replace_on_slide(
    slides,
    presentation_id: str,
    page_object_id: str,
    mapping: Dict[str, Optional[str]],
) -> None:
    """
    Делает replaceAllText ТОЛЬКО на указанной странице (pageObjectId),
    чтобы одинаковые плейсхолдеры на соседних слайдах не затирали друг друга.
    Значения None -> пустая строка.
    """
    requests = []
    for key, value in mapping.items():
        text = "" if value is None else str(value)
        tag = "{{" + str(key) + "}}"  # вместо f"{{{{{key}}}}}"
        requests.append(
            {
                "replaceAllText": {
                    "containsText": {"text": tag, "matchCase": True},
                    "replaceText": text,
                    "pageObjectIds": [page_object_id],
                }
            }
        )

    if requests:
        slides.presentations().batchUpdate(
            presentationId=presentation_id, body={"requests": requests}
        ).execute()


def ensure_pages(
    slides,
    presentation_id: str,
    base_page_id: str,
    pages_total: int,
) -> List[str]:
    """
    Гарантирует наличие pages_total слайдов по одному шаблону: первый — base_page_id,
    остальные — дубли первого. Возвращает список pageObjectId (в порядке).
    """
    if pages_total <= 0:
        return []

    page_ids = [base_page_id]
    for _ in range(pages_total - 1):
        new_id = duplicate_slide(slides, presentation_id, base_page_id)
        page_ids.append(new_id)
    return page_ids


# ─────────────────────────────────────────────────────────────────────────────
# ВЫСОКОУРОВНЕВЫЕ ОБЁРТКИ ДЛЯ ОТЧЁТНОГО ПАЙПЛАЙНА


import time

from googleapiclient.errors import HttpError


def prepare_presentation_from_template(
    template_id: str,
    title: str,
    parent_folder_id: str,
) -> Tuple[str, List[str]]:
    drive, slides, _ = build_services()
    pres_id = copy_slides_to_folder(drive, template_id, title, parent_folder_id)

    # Ретрай: сразу после copy презентация иногда не «готова» для Slides API
    last_err = None
    for attempt in range(6):  # ~0s, 0.5s, 1.0s, 1.5s, 2.0s, 2.5s
        try:
            page_ids = get_presentation_page_ids(slides, pres_id)
            return pres_id, page_ids
        except HttpError as e:
            # 400 "This operation is not supported for this document"
            if e.resp.status == 400 and b"not supported for this document" in (
                e.content or b""
            ):
                last_err = e
                time.sleep(0.5 * attempt)
                continue
            raise
    # Если так и не готово — даём более человекочитаемое объяснение
    mime = get_file_mime_type(drive, pres_id)
    raise RuntimeError(
        f"Slides is not ready for this file or wrong type. mimeType={mime}. "
        f"Original error: {last_err}"
    )


def render_and_export_pdf(
    presentation_id: str,
    per_slide_mappings: List[Dict[str, Optional[str]]],
    base_slide_index: int = 0,
) -> bytes:
    """
    Заполняет презентацию, создаёт нужное число копий базового слайда (по количеству маппингов),
    заменяет плейсхолдеры на каждой странице и экспортирует PDF (байты).

    per_slide_mappings: список словарей значений для каждого слайда в порядке.
    base_slide_index: индекс слайда-шаблона (обычно 0).
    """
    drive, slides, _ = build_services()

    page_ids = get_presentation_page_ids(slides, presentation_id)
    if not page_ids:
        raise RuntimeError("Presentation has no slides")
    base_id = page_ids[base_slide_index]

    # Гарантируем нужное количество страниц
    page_ids_final = ensure_pages(
        slides, presentation_id, base_id, len(per_slide_mappings)
    )

    # Заполняем по страницам
    for page_id, mapping in zip(page_ids_final, per_slide_mappings):
        replace_on_slide(slides, presentation_id, page_id, mapping)

    # Экспорт в PDF
    pdf_bytes = export_slides_to_pdf(drive, presentation_id)
    return pdf_bytes
