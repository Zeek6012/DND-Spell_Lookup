import os
import re
import io
import sqlite3
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

st.set_page_config(page_title="D&D Lookup", page_icon="📚", layout="wide")

DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "lookup.db")

TABLE_SPELLS = "spells"
TABLE_FEATS = "feats"

DEFAULT_SPELLS_XLSX = os.path.join(DATA_DIR, "spells.xlsx")
DEFAULT_FEATS_XLSX = os.path.join(DATA_DIR, "feats.xlsx")

KNOWN_CLASSES = [
    "Barbarian","Bard","Cleric","Druid","Fighter","Monk","Paladin",
    "Ranger","Rogue","Sorcerer","Warlock","Wizard","Artificer"
]
TRUTHY = {"x","yes","y","true","1","✓","✅"}
BLANK_MARKERS = {"", "_", "-", "—", "–", "n/a", "na", "none", "null", "0/0"}


# ---------------- Shared helpers ----------------
def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def connect_db() -> sqlite3.Connection:
    ensure_data_dir()
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def table_exists(con: sqlite3.Connection, table: str) -> bool:
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
    return cur.fetchone() is not None


def get_table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    cur = con.cursor()
    cur.execute(f'PRAGMA table_info("{table}");')
    return [r[1] for r in cur.fetchall()]


def ensure_columns(table: str, required_cols: List[str]) -> None:
    con = connect_db()
    try:
        if not table_exists(con, table):
            return
        existing = set(get_table_columns(con, table))
        for col in required_cols:
            if col not in existing:
                con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT DEFAULT "";')
        con.commit()
    finally:
        con.close()


def norm(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def is_blank(v: str) -> bool:
    t = str(v).strip()
    if not t:
        return True
    if t.strip().lower() in BLANK_MARKERS:
        return True
    if re.fullmatch(r"[_\-\—\–\s]+", t):
        return True
    return False


def clean(v: str) -> str:
    t = "" if v is None else str(v)
    t = t.strip()
    return "" if is_blank(t) else t


def is_truthy(val: str) -> bool:
    v = str(val).strip().lower()
    if v in TRUTHY:
        return True
    if v and v not in {"0", "no", "false", "n"} and len(v) <= 3:
        return True
    return False


def find_col_candidates(df: pd.DataFrame, candidates: List[str]) -> List[str]:
    cols = {norm(c): c for c in df.columns}
    matches = set()

    for cand in candidates:
        cn = norm(cand)
        if cn in cols:
            matches.add(cols[cn])

    for cand in candidates:
        cn = norm(cand)
        for n, orig in cols.items():
            if cn and cn in n:
                matches.add(orig)

    return list(matches)


def best_matching_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    matches = find_col_candidates(df, candidates)
    if not matches:
        return None

    def nonblank_count(col: str) -> int:
        s = df[col].astype(str).map(clean)
        return int((s != "").sum())

    ranked = sorted(matches, key=nonblank_count, reverse=True)
    return ranked[0]


@st.cache_data(show_spinner=False)
def load_df(table: str) -> pd.DataFrame:
    con = connect_db()
    try:
        return pd.read_sql_query(f'SELECT * FROM "{table}";', con).fillna("")
    finally:
        con.close()


@st.cache_data(show_spinner=False)
def build_search_blob(df: pd.DataFrame, exclude_cols: Tuple[str, ...]) -> pd.Series:
    # Safe for empty tables
    if df is None or len(df) == 0:
        return pd.Series([], dtype=str)
    cols = [c for c in df.columns if c not in set(exclude_cols)]
    if len(cols) == 0:
        return pd.Series([], dtype=str)
    s = df[cols].astype(str).agg(" ".join, axis=1)
    if not isinstance(s, pd.Series):
        return pd.Series([], dtype=str)
    return s.astype(str).str.lower()


def update_row(table: str, row_id: int, updates: Dict[str, str]) -> None:
    if not updates:
        return
    con = connect_db()
    try:
        cols = list(updates.keys())
        vals = [updates[c] for c in cols]
        set_clause = ", ".join([f'"{c}"=?' for c in cols])
        sql = f'UPDATE "{table}" SET {set_clause} WHERE id=?;'
        con.execute(sql, (*vals, row_id))
        con.commit()
    finally:
        con.close()


def insert_row(table: str, values: Dict[str, str]) -> int:
    con = connect_db()
    try:
        cols = get_table_columns(con, table)
        cur = con.cursor()
        cur.execute(f'SELECT COALESCE(MAX(id), 0) + 1 FROM "{table}";')
        new_id = int(cur.fetchone()[0])

        row = {c: "" for c in cols}
        row["id"] = new_id
        for k, v in values.items():
            if k in row:
                row[k] = v

        col_list = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join(["?"] * len(cols))
        con.execute(
            f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders});',
            [row[c] for c in cols]
        )
        con.commit()
        return new_id
    finally:
        con.close()


def drop_table(table: str) -> None:
    con = connect_db()
    try:
        con.execute(f'DROP TABLE IF EXISTS "{table}";')
        con.commit()
    finally:
        con.close()


def render_desc_with_extra_blank_lines(text: str) -> str:
    if text is None:
        return ""
    lines = str(text).splitlines()
    out = []
    blank_run = 0
    for line in lines:
        if line.strip() == "":
            blank_run += 1
            if blank_run == 1:
                out.append("")
            else:
                out.append("&nbsp;")
        else:
            blank_run = 0
            out.append(line)
    return "\n".join(out)


def excel_bytes(data_df: pd.DataFrame, instructions: List[str]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        data_df.to_excel(w, index=False, sheet_name="Data")
        pd.DataFrame({"Instructions": instructions}).to_excel(w, index=False, sheet_name="Instructions")
    return buf.getvalue()


# ---------------- Spells ----------------
def build_classes_from_columns(df: pd.DataFrame) -> pd.DataFrame:
    # If checkbox columns exist, convert them into _Classes
    norm_to_orig = {norm(c): c for c in df.columns}
    class_cols = {}
    for cls in KNOWN_CLASSES:
        k = norm(cls)
        if k in norm_to_orig:
            class_cols[cls] = norm_to_orig[k]
    if not class_cols:
        return df

    def row_classes(row) -> str:
        hits = []
        for cls, col in class_cols.items():
            if is_truthy(row.get(col, "")):
                hits.append(cls)
        return "; ".join(hits)

    df["_Classes"] = df.apply(row_classes, axis=1)
    return df


def drop_class_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [c for c in KNOWN_CLASSES if c in df.columns]
    return df.drop(columns=cols_to_drop, errors="ignore")


def import_spells_excel_to_db(xlsx_path: str) -> None:
    df = pd.read_excel(xlsx_path, dtype=str).fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    df = build_classes_from_columns(df)
    df = drop_class_columns(df)

    col_name = best_matching_col(df, ["spell name", "name", "spell"])
    if not col_name:
        raise ValueError("Could not find a spell name column (e.g., 'Spell Name' or 'Name').")

    if "Description" not in df.columns:
        df["Description"] = ""

    # Drop unnamed spells
    df["_name_clean"] = df[col_name].astype(str).map(clean)
    df = df[df["_name_clean"] != ""].drop(columns=["_name_clean"], errors="ignore")

    df.insert(0, "id", range(1, len(df) + 1))

    con = connect_db()
    try:
        df.to_sql(TABLE_SPELLS, con, if_exists="replace", index=False)
    finally:
        con.close()

    ensure_columns(TABLE_SPELLS, [
        "Description","Save/Attack Roll","Concentration","Damage Type",
        "Utility Effect","Source","Page","_Classes"
    ])


def format_page(pg: str) -> str:
    pg = clean(pg)
    if not pg:
        return ""
    if pg.lower().startswith("pg."):
        return pg
    if pg.isdigit():
        return f"pg. {pg}"
    return f"pg. {pg}"


# ---------------- Feats ----------------
FEAT_COLS = ["Feat Name", "Race", "Class", "Subclass", "Description"]


def ensure_feats_table() -> None:
    con = connect_db()
    try:
        if table_exists(con, TABLE_FEATS):
            return
        empty = pd.DataFrame(columns=["id"] + FEAT_COLS)
        empty.to_sql(TABLE_FEATS, con, if_exists="replace", index=False)
    finally:
        con.close()


def import_feats_excel_to_db(xlsx_path: str) -> None:
    df = pd.read_excel(xlsx_path, dtype=str).fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    col_name = best_matching_col(df, ["feat name", "name", "feat"])
    if not col_name:
        raise ValueError("Could not find a feat name column (e.g., 'Feat Name' or 'Name').")

    col_race = best_matching_col(df, ["race"])
    col_class = best_matching_col(df, ["class"])
    col_sub = best_matching_col(df, ["subclass", "sub-class", "sub class"])
    col_desc = best_matching_col(df, ["description", "details", "text"])

    out = pd.DataFrame()
    out["Feat Name"] = df[col_name].astype(str).map(clean)
    out["Race"] = df[col_race].astype(str).map(clean) if col_race else ""
    out["Class"] = df[col_class].astype(str).map(clean) if col_class else ""
    out["Subclass"] = df[col_sub].astype(str).map(clean) if col_sub else ""
    out["Description"] = df[col_desc].astype(str) if col_desc else ""

    out = out[out["Feat Name"].map(clean) != ""].copy()
    out.insert(0, "id", range(1, len(out) + 1))

    con = connect_db()
    try:
        out.to_sql(TABLE_FEATS, con, if_exists="replace", index=False)
    finally:
        con.close()

    ensure_columns(TABLE_FEATS, FEAT_COLS)


# ---------------- App bootstrap/state ----------------
def init_state():
    st.session_state.setdefault("spell_last_query", "")
    st.session_state.setdefault("selected_spell_id", None)
    st.session_state.setdefault("spell_editing", False)
    st.session_state.setdefault("spell_confirming", False)
    st.session_state.setdefault("spell_pending_updates", {})

    st.session_state.setdefault("feat_last_query", "")
    st.session_state.setdefault("selected_feat_id", None)
    st.session_state.setdefault("feat_editing", False)
    st.session_state.setdefault("feat_confirming", False)
    st.session_state.setdefault("feat_pending_updates", {})


def ensure_bootstrap():
    ensure_data_dir()

    con = connect_db()
    try:
        has_spells = table_exists(con, TABLE_SPELLS)
    finally:
        con.close()

    if (not has_spells) and os.path.exists(DEFAULT_SPELLS_XLSX):
        import_spells_excel_to_db(DEFAULT_SPELLS_XLSX)
        st.cache_data.clear()

    ensure_feats_table()
    con = connect_db()
    try:
        has_feats = table_exists(con, TABLE_FEATS)
    finally:
        con.close()
    if has_feats and os.path.exists(DEFAULT_FEATS_XLSX):
        # Only import feats.xlsx if feats table exists but is empty
        df = load_df(TABLE_FEATS)
        if len(df) == 0:
            import_feats_excel_to_db(DEFAULT_FEATS_XLSX)
            st.cache_data.clear()


# ---------------- UI: Spells ----------------
def spells_lookup_page():
    st.title("📜 Spell Lookup")

    con = connect_db()
    try:
        if not table_exists(con, TABLE_SPELLS):
            st.info("No Spells found yet. Go to **Manage Spells** to add or bulk upload.", icon="✨")
            return
    finally:
        con.close()

    df = drop_class_columns(load_df(TABLE_SPELLS))

    col_name = best_matching_col(df, ["spell name", "name", "spell"])
    if not col_name:
        st.info("No Spells found yet. Go to **Manage Spells** to add or bulk upload.", icon="✨")
        return

    df["_name_clean"] = df[col_name].astype(str).map(clean)
    df = df[df["_name_clean"] != ""].copy()
    if df.empty:
        st.info("No Spells found yet. Go to **Manage Spells** to add or bulk upload.", icon="✨")
        return

    col_level = best_matching_col(df, ["level", "spell level", "lvl"])
    col_school = best_matching_col(df, ["school", "magic school"])
    col_classes = best_matching_col(df, ["_classes", "classes", "class"]) or "_Classes"

    col_cast = best_matching_col(df, ["casting time", "cast time"])
    col_range = best_matching_col(df, ["range"])
    col_duration = best_matching_col(df, ["duration"])
    col_components = best_matching_col(df, ["components"])

    col_save_attack = best_matching_col(df, ["save/attack roll", "save or attack roll", "save attack roll"]) or "Save/Attack Roll"
    col_conc = best_matching_col(df, ["concentration"]) or "Concentration"
    col_dmg = best_matching_col(df, ["damage type"]) or "Damage Type"
    col_utility = best_matching_col(df, ["utility effect"]) or "Utility Effect"

    col_desc = "Description"
    col_source = best_matching_col(df, ["source", "reference", "book", "citation", "src"]) or "Source"
    col_page = best_matching_col(df, ["page", "pg", "page number", "pages", "pg#","pg #","p#"]) or "Page"

    q = st.text_input(
        "Search",
        value=st.session_state.get("spell_last_query", ""),
        placeholder="Type anything (Bard, Acid Splash, Abjuration, 1 minute...)",
        key="spell_search_box",
    )
    st.session_state["spell_last_query"] = q

    if not q.strip():
        st.info("Search for a spell to begin.")
        st.session_state["selected_spell_id"] = None
        st.session_state["spell_editing"] = False
        st.session_state["spell_confirming"] = False
        return

    blob = build_search_blob(df, exclude_cols=("Description",))
    view = df[blob.str.contains(re.escape(q.strip().lower()), na=False)].copy()

    if view.empty:
        st.warning("No matches.")
        return

    def label_row(row) -> str:
        name = clean(row.get(col_name, ""))
        lvl = clean(row.get(col_level, "")) if col_level else ""
        school = clean(row.get(col_school, "")) if col_school else ""
        meta = []
        if lvl:
            meta.append(f"Lv {lvl}")
        if school:
            meta.append(school)
        return f"{name} — " + " • ".join(meta) if meta else name

    view = view.sort_values(by=col_name, key=lambda s: s.astype(str).str.lower())
    options = list(view["id"].astype(int).tolist())
    labels = {int(r["id"]): label_row(r) for _, r in view.iterrows()}

    default_index = 0
    if st.session_state["selected_spell_id"] in options:
        default_index = options.index(st.session_state["selected_spell_id"])

    picked_id = st.selectbox(
        "Results",
        options=options,
        index=default_index,
        format_func=lambda i: labels.get(int(i), str(i)),
        key="spell_results_select",
    )
    st.session_state["selected_spell_id"] = int(picked_id)

    selected = view[view["id"].astype(int) == int(picked_id)].iloc[0].to_dict()

    def get(col: Optional[str]) -> str:
        if not col:
            return ""
        return clean(selected.get(col, ""))

    spell_name = get(col_name)
    lvl = get(col_level)
    school = get(col_school)
    classes_line = get(col_classes)

    cast = get(col_cast)
    rng = get(col_range)
    dur = get(col_duration)
    comps = get(col_components)

    save_attack = get(col_save_attack)
    conc = get(col_conc)
    dmg = get(col_dmg)
    util = get(col_utility)

    raw_desc = str(selected.get(col_desc, "") or "")
    src = get(col_source)
    pg = format_page(get(col_page))

    b1, _, _ = st.columns([1, 1, 4])
    with b1:
        if not st.session_state["spell_editing"]:
            if st.button("Edit", type="primary", key="spell_edit_btn"):
                st.session_state["spell_editing"] = True
                st.session_state["spell_confirming"] = False
                st.session_state["spell_pending_updates"] = {}
                st.rerun()
        else:
            if st.button("Cancel", key="spell_cancel_btn"):
                st.session_state["spell_editing"] = False
                st.session_state["spell_confirming"] = False
                st.session_state["spell_pending_updates"] = {}
                st.rerun()

    # Card
    with st.container(border=True):
        st.markdown(f"## {spell_name}")

        # subtitle
        sub_parts = []
        if lvl:
            sub_parts.append(lvl)
            if lvl.isdigit():
                sub_parts.append("level")
        if school:
            sub_parts.append(school)
        sub = " ".join(sub_parts).strip() or "Spell"
        st.markdown(f"*{sub}*")

        if not is_blank(classes_line):
            st.markdown(f"*{classes_line}*")

        def meta_line(label: str, val: str):
            if not is_blank(val):
                st.markdown(f"**{label}:** {val}")

        meta_line("Casting Time", cast)
        meta_line("Range", rng)
        meta_line("Duration", dur)
        meta_line("Components", comps)
        meta_line("Save/Attack Roll", save_attack)
        meta_line("Concentration", conc)
        meta_line("Damage Type", dmg)
        meta_line("Utility Effect", util)

        if not st.session_state["spell_editing"]:
            if raw_desc.strip():
                st.divider()
                st.markdown(render_desc_with_extra_blank_lines(raw_desc), unsafe_allow_html=True)

            if not is_blank(src) or not is_blank(pg):
                st.divider()
                if not is_blank(src):
                    st.markdown(f"**Source:** {src}")
                if not is_blank(pg):
                    st.markdown(f"{pg}")
        else:
            updates: Dict[str, str] = {}

            new_desc = st.text_area(
                "Description (Markdown)",
                value=raw_desc,
                height=340,
                help="Markdown supports **bold**. Underline via <u>text</u>. Extra blank lines preserved.",
                key="spell_desc_editor",
            )
            if new_desc != raw_desc:
                updates[col_desc] = new_desc

            for col in df.columns:
                if col in {"id", "_name_clean", col_desc}:
                    continue
                current = str(selected.get(col, "") if selected.get(col, "") is not None else "")
                new = st.text_input(col, value=current, key=f"spell_edit_{col}")
                if new != current:
                    updates[col] = new

            st.session_state["spell_pending_updates"] = updates

            if st.button("Save", type="primary", disabled=(len(updates) == 0), key="spell_save_btn"):
                st.session_state["spell_confirming"] = True
                st.rerun()

            if st.session_state["spell_confirming"]:
                st.warning(f"Save changes to **{spell_name}**?", icon="⚠️")
                c1, c2 = st.columns([1, 1])
                with c1:
                    if st.button("✅ Confirm Save", type="primary", key="spell_confirm_save_btn"):
                        update_row(TABLE_SPELLS, int(picked_id), st.session_state["spell_pending_updates"])
                        st.cache_data.clear()
                        st.session_state["spell_editing"] = False
                        st.session_state["spell_confirming"] = False
                        st.session_state["spell_pending_updates"] = {}
                        st.success("Saved.")
                        st.rerun()
                with c2:
                    if st.button("↩ Cancel Save", key="spell_cancel_save_btn"):
                        st.session_state["spell_confirming"] = False
                        st.rerun()


            st.divider()
            st.subheader("Danger Zone")
            st.caption("Deletes ONLY this spell (not the whole table).")

            spell_del_confirm = st.text_input(
                f'Type DELETE to enable deleting “{spell_name}”',
                value="",
                key=f"spell_delete_confirm_{picked_id}",
                disabled=st.session_state.get("spell_confirming", False),
            )
            if st.button(
                "🗑️ Delete This Spell",
                type="primary",
                disabled=(spell_del_confirm.strip().upper() != "DELETE") or st.session_state.get("spell_confirming", False),
                key=f"spell_delete_btn_{picked_id}",
                use_container_width=True,
            ):
                delete_row(TABLE_SPELLS, int(picked_id))
                st.cache_data.clear()
                st.session_state["spell_editing"] = False
                st.session_state["spell_confirming"] = False
                st.session_state["spell_pending_updates"] = {}
                st.success("Spell deleted.")
                st.rerun()



def delete_row(table: str, row_id: int) -> None:
    con = connect_db()
    try:
        con.execute(f"DELETE FROM {table} WHERE id = ?", (row_id,))
        con.commit()
    finally:
        con.close()

def manage_spells_page():
    st.title("Manage Spells")
    st.info(
        "- Use **_Classes** like `Wizard; Sorcerer` (semicolon-separated).\n"
        "- Leave blanks blank (don’t use `_`).\n"
        "- **Page** can be `239` or `pg. 239`.\n"
        "- **Description** supports Markdown (`**bold**`) and underline with `<u>text</u>`.\n"
        "- Template + export use the same format. Class checkbox columns are not used.\n",
        icon="🧾"
    )

    con = connect_db()
    try:
        has_spells = table_exists(con, TABLE_SPELLS)
    finally:
        con.close()

    if has_spells:
        df = drop_class_columns(load_df(TABLE_SPELLS))
        export_df = df.drop(columns=["id"], errors="ignore")
    else:
        export_df = pd.DataFrame(columns=[
            "Spell Name","Level","School","_Classes","Casting Time","Range","Duration","Components",
            "Save/Attack Roll","Concentration","Damage Type","Utility Effect","Description","Source","Page"
        ])

    # Template matches export format (plus 1 example row)
    template = export_df.copy().head(0)
    ex = {c: "" for c in template.columns}
    name_col = best_matching_col(template, ["spell name", "name", "spell"]) or ("Spell Name" if "Spell Name" in template.columns else template.columns[0])
    ex[name_col] = "Acid Splash"
    if "Level" in template.columns: ex["Level"] = "0"
    if "School" in template.columns: ex["School"] = "Conjuration"
    if "_Classes" in template.columns: ex["_Classes"] = "Sorcerer; Wizard"
    if "Range" in template.columns: ex["Range"] = "60 feet"
    if "Save/Attack Roll" in template.columns: ex["Save/Attack Roll"] = "DEX save"
    if "Damage Type" in template.columns: ex["Damage Type"] = "Acid"
    if "Description" in template.columns: ex["Description"] = "Example.\n\n**Bold** and <u>underline</u> supported."
    if "Source" in template.columns: ex["Source"] = "PHB"
    if "Page" in template.columns: ex["Page"] = "239"
    template = pd.concat([template, pd.DataFrame([ex])], ignore_index=True)

    st.subheader("Downloads")
    st.download_button(
        "⬇️ Download Blank Spells Template (.xlsx)",
        data=excel_bytes(template, [
            "Fill the Data sheet only.",
            "Use _Classes (semicolon-separated).",
            "Leave blanks blank (do not use _).",
            "Description supports Markdown; underline via <u>text</u>.",
            "Upload via Bulk Import / Replace."
        ]),
        file_name="spells_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    st.download_button(
        "⬇️ Download Current Spells (.xlsx)",
        data=excel_bytes(export_df, ["Full export of your spells."]),
        file_name="spells_export.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    st.divider()
    st.subheader("Bulk Import / Replace")
    up = st.file_uploader("Upload .xlsx", type=["xlsx"], key="spells_uploader")
    save_copy = st.checkbox("Also save a copy to data/spells.xlsx", value=True, key="spells_save_copy")

    if up is not None and st.button("Import Spells (Replace DB)", type="primary", use_container_width=True):
        df_in = pd.read_excel(up, dtype=str).fillna("")
        df_in.columns = [str(c).strip() for c in df_in.columns]
        df_in = build_classes_from_columns(df_in)
        df_in = drop_class_columns(df_in)
        if "Description" not in df_in.columns:
            df_in["Description"] = ""

        if save_copy:
            df_in.to_excel(DEFAULT_SPELLS_XLSX, index=False)

        tmp = os.path.join(DATA_DIR, "_spells_upload_tmp.xlsx")
        df_in.to_excel(tmp, index=False)
        import_spells_excel_to_db(tmp)
        st.cache_data.clear()
        st.success("Imported successfully.")

    st.divider()
    st.subheader("Add New Spell (no Excel needed)")
    with st.form("add_spell_form", clear_on_submit=True):
        name = st.text_input("Spell Name*", value="")
        classes = st.text_input("_Classes", value="")
        desc = st.text_area("Description (Markdown)", value="", height=220)
        submitted = st.form_submit_button("Create Spell", type="primary")

        if submitted:
            if not name.strip():
                st.error("Spell Name is required.")
            else:
                # Ensure spells table exists with at least base schema
                con = connect_db()
                try:
                    if not table_exists(con, TABLE_SPELLS):
                        base = pd.DataFrame(columns=["id","Spell Name","_Classes","Description"])
                        base.to_sql(TABLE_SPELLS, con, if_exists="replace", index=False)
                finally:
                    con.close()

                ensure_columns(TABLE_SPELLS, [
                    "Spell Name","_Classes","Description","Level","School","Casting Time","Range","Duration","Components",
                    "Save/Attack Roll","Concentration","Damage Type","Utility Effect","Source","Page"
                ])

                live = load_df(TABLE_SPELLS)
                name_col = best_matching_col(live, ["spell name", "name", "spell"]) or "Spell Name"
                new_id = insert_row(TABLE_SPELLS, {
                    name_col: name,
                    "_Classes": classes,
                    "Description": desc
                })
                st.cache_data.clear()
                st.success(f"Created spell (ID {new_id}).")

    st.divider()
    st.subheader("Danger Zone")
    st.caption("Deletes ALL spells. Export first if you want a backup.")
    confirm_text = st.text_input("Type CONFIRM to enable delete", value="", key="spells_clear_confirm")
    if st.button("🗑️ CLEAR SPELLS (IRREVERSIBLE)", type="primary", disabled=(confirm_text.strip().upper() != "CONFIRM"), use_container_width=True):
        drop_table(TABLE_SPELLS)
        st.cache_data.clear()
        st.success("Spells cleared.")


# ---------------- UI: Feats ----------------
def feats_lookup_page():
    st.title("✨ Feat Lookup")
    ensure_feats_table()

    df = load_df(TABLE_FEATS)
    if df.empty:
        st.info("No feats found yet. Go to **Manage Feats** to add or bulk upload.", icon="✨")
        return

    if "Feat Name" not in df.columns:
        st.error("Feats table is missing 'Feat Name'.")
        return

    df["_name_clean"] = df["Feat Name"].astype(str).map(clean)
    df = df[df["_name_clean"] != ""].copy()
    if df.empty:
        st.info("No feats found yet. Go to **Manage Feats** to add or bulk upload.", icon="✨")
        return

    q = st.text_input(
        "Search",
        value=st.session_state.get("feat_last_query", ""),
        placeholder="Type anything (Sharpshooter, Elf, Fighter...)",
        key="feat_search_box",
    )
    st.session_state["feat_last_query"] = q

    if not q.strip():
        st.info("Search for a feat to begin.")
        st.session_state["selected_feat_id"] = None
        st.session_state["feat_editing"] = False
        st.session_state["feat_confirming"] = False
        return

    blob = build_search_blob(df, exclude_cols=("Description",))
    view = df[blob.str.contains(re.escape(q.strip().lower()), na=False)].copy()

    if view.empty:
        st.warning("No matches.")
        return

    view = view.sort_values(by="Feat Name", key=lambda s: s.astype(str).str.lower())
    options = list(view["id"].astype(int).tolist())

    default_index = 0
    if st.session_state["selected_feat_id"] in options:
        default_index = options.index(st.session_state["selected_feat_id"])

    picked_id = st.selectbox(
        "Results",
        options=options,
        index=default_index,
        format_func=lambda i: clean(view[view["id"].astype(int) == int(i)].iloc[0]["Feat Name"]),
        key="feat_results_select",
    )
    st.session_state["selected_feat_id"] = int(picked_id)

    selected = view[view["id"].astype(int) == int(picked_id)].iloc[0].to_dict()

    feat_name = clean(selected.get("Feat Name", ""))
    race = clean(selected.get("Race", ""))
    clazz = clean(selected.get("Class", ""))
    subclass = clean(selected.get("Subclass", ""))
    raw_desc = str(selected.get("Description", "") or "")

    b1, _, _ = st.columns([1, 1, 4])
    with b1:
        if not st.session_state["feat_editing"]:
            if st.button("Edit", type="primary", key="feat_edit_btn"):
                st.session_state["feat_editing"] = True
                st.session_state["feat_confirming"] = False
                st.session_state["feat_pending_updates"] = {}
                st.rerun()
        else:
            if st.button("Cancel", key="feat_cancel_btn"):
                st.session_state["feat_editing"] = False
                st.session_state["feat_confirming"] = False
                st.session_state["feat_pending_updates"] = {}
                st.rerun()

    with st.container(border=True):
        st.markdown(f"## {feat_name}")

        def meta_line(label: str, val: str):
            if not is_blank(val):
                st.markdown(f"**{label}:** {val}")

        meta_line("Race", race)
        meta_line("Class", clazz)
        meta_line("Subclass", subclass)

        if not st.session_state["feat_editing"]:
            if raw_desc.strip():
                st.divider()
                st.markdown(render_desc_with_extra_blank_lines(raw_desc), unsafe_allow_html=True)
        else:
            updates: Dict[str, str] = {}

            new_desc = st.text_area(
                "Description (Markdown)",
                value=raw_desc,
                height=320,
                help="Markdown supports **bold**. Underline via <u>text</u>. Extra blank lines preserved.",
                key="feat_desc_editor",
            )
            if new_desc != raw_desc:
                updates["Description"] = new_desc

            for col in ["Feat Name","Race","Class","Subclass"]:
                current = str(selected.get(col, "") if selected.get(col, "") is not None else "")
                new = st.text_input(col, value=current, key=f"feat_edit_{col}")
                if new != current:
                    updates[col] = new

            st.session_state["feat_pending_updates"] = updates

            if st.button("Save", type="primary", disabled=(len(updates) == 0), key="feat_save_btn"):
                st.session_state["feat_confirming"] = True
                st.rerun()

            if st.session_state["feat_confirming"]:
                st.warning(f"Save changes to **{feat_name}**?", icon="⚠️")
                c1, c2 = st.columns([1, 1])
                with c1:
                    if st.button("✅ Confirm Save", type="primary", key="feat_confirm_save_btn"):
                        update_row(TABLE_FEATS, int(picked_id), st.session_state["feat_pending_updates"])
                        st.cache_data.clear()
                        st.session_state["feat_editing"] = False
                        st.session_state["feat_confirming"] = False
                        st.session_state["feat_pending_updates"] = {}
                        st.success("Saved.")
                        st.rerun()
                with c2:
                    if st.button("↩ Cancel Save", key="feat_cancel_save_btn"):
                        st.session_state["feat_confirming"] = False
                        st.rerun()


            st.divider()
            st.subheader("Danger Zone")
            st.caption("Deletes ONLY this feat (not the whole table).")

            feat_del_confirm = st.text_input(
                f'Type DELETE to enable deleting “{feat_name}”',
                value="",
                key=f"feat_delete_confirm_{picked_id}",
                disabled=st.session_state.get("feat_confirming", False),
            )
            if st.button(
                "🗑️ Delete This Feat",
                type="primary",
                disabled=(feat_del_confirm.strip().upper() != "DELETE") or st.session_state.get("feat_confirming", False),
                key=f"feat_delete_btn_{picked_id}",
                use_container_width=True,
            ):
                delete_row(TABLE_FEATS, int(picked_id))
                st.cache_data.clear()
                st.session_state["feat_editing"] = False
                st.session_state["feat_confirming"] = False
                st.session_state["feat_pending_updates"] = {}
                st.success("Feat deleted.")
                st.rerun()


def manage_feats_page():
    st.title("Manage Feats")
    st.info(
        "- Columns: Feat Name, Race, Class, Subclass, Description.\n"
        "- Leave blanks blank (don’t use `_`).\n"
        "- Description supports Markdown and underline with `<u>text</u>`.\n",
        icon="🧾"
    )

    ensure_feats_table()
    df = load_df(TABLE_FEATS)
    export_df = df.drop(columns=["id"], errors="ignore")
    if export_df.empty:
        export_df = pd.DataFrame(columns=FEAT_COLS)

    template = export_df.copy().head(0)
    ex = {c: "" for c in template.columns}
    ex["Feat Name"] = "Sharpshooter"
    ex["Class"] = "Fighter; Ranger"
    ex["Description"] = "Example.\n\n**Bold** and <u>underline</u> supported."
    template = pd.concat([template, pd.DataFrame([ex])], ignore_index=True)

    st.subheader("Downloads")
    st.download_button(
        "⬇️ Download Blank Feats Template (.xlsx)",
        data=excel_bytes(template, [
            "Fill the Data sheet only.",
            "Use semicolons to separate multiples (e.g., Fighter; Ranger).",
            "Leave blanks blank (do not use _).",
            "Description supports Markdown; underline via <u>text</u>.",
            "Upload via Bulk Import / Replace."
        ]),
        file_name="feats_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    st.download_button(
        "⬇️ Download Current Feats (.xlsx)",
        data=excel_bytes(export_df, ["Full export of your feats."]),
        file_name="feats_export.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    st.divider()
    st.subheader("Bulk Import / Replace")
    up = st.file_uploader("Upload .xlsx", type=["xlsx"], key="feats_uploader")
    save_copy = st.checkbox("Also save a copy to data/feats.xlsx", value=True, key="feats_save_copy")

    if up is not None and st.button("Import Feats (Replace DB)", type="primary", use_container_width=True):
        df_in = pd.read_excel(up, dtype=str).fillna("")
        df_in.columns = [str(c).strip() for c in df_in.columns]

        if save_copy:
            df_in.to_excel(DEFAULT_FEATS_XLSX, index=False)

        tmp = os.path.join(DATA_DIR, "_feats_upload_tmp.xlsx")
        df_in.to_excel(tmp, index=False)
        import_feats_excel_to_db(tmp)
        st.cache_data.clear()
        st.success("Imported successfully.")

    st.divider()
    st.subheader("Add New Feat (no Excel needed)")
    with st.form("add_feat_form", clear_on_submit=True):
        name = st.text_input("Feat Name*", value="")
        race = st.text_input("Race", value="")
        clazz = st.text_input("Class", value="")
        subclass = st.text_input("Subclass", value="")
        desc = st.text_area("Description (Markdown)", value="", height=240)
        submitted = st.form_submit_button("Create Feat", type="primary")

        if submitted:
            if not name.strip():
                st.error("Feat Name is required.")
            else:
                new_id = insert_row(TABLE_FEATS, {
                    "Feat Name": name,
                    "Race": race,
                    "Class": clazz,
                    "Subclass": subclass,
                    "Description": desc,
                })
                st.cache_data.clear()
                st.success(f"Created feat (ID {new_id}).")

    st.divider()
    st.subheader("Danger Zone")
    st.caption("Deletes ALL feats. Export first if you want a backup.")
    confirm_text = st.text_input("Type CONFIRM to enable delete", value="", key="feats_clear_confirm")
    if st.button("🗑️ CLEAR FEATS (IRREVERSIBLE)", type="primary", disabled=(confirm_text.strip().upper() != "CONFIRM"), use_container_width=True):
        drop_table(TABLE_FEATS)
        ensure_feats_table()
        st.cache_data.clear()
        st.success("Feats cleared.")


def main():
    init_state()
    ensure_bootstrap()

    with st.sidebar:
        page = st.radio("Menu", ["Spell Lookup", "Feat Lookup", "Manage Spells", "Manage Feats"], index=0)

    if page == "Spell Lookup":
        spells_lookup_page()
    elif page == "Feat Lookup":
        feats_lookup_page()
    elif page == "Manage Spells":
        manage_spells_page()
    elif page == "Manage Feats":
        manage_feats_page()


if __name__ == "__main__":
    main()

