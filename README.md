# CV — Tran Nhat Nam (Product Data Engineer)

This repo builds and maintains a LaTeX-based CV (Harvard/ATS-friendly) compiled to PDF
with **Tectonic**. Two versions are maintained:

- `main.tex` → `main.pdf`: the main CV (2 pages), tailored for Senior Data Engineer roles.
- `master.tex` → `master.pdf`: the Master CV (2 pages), keyword-rich — the source used to
  fine-tune a version per job description.

---

## Requirements

- **macOS** (tested on Apple Silicon).
- **Homebrew** (`brew`).
- The **Libertinus** font does **not** need to be installed manually — Tectonic fetches it
  from CTAN automatically on the first build.

## Tooling setup

### 1. Install Tectonic (the LaTeX engine)

```bash
brew install tectonic
```

Verify:

```bash
tectonic --version
# Tectonic 0.17.0 (or newer)
```

> Tectonic uses the **XeTeX** engine, so `fontspec`, system fonts and the `libertinus`
> package all work out of the box. Missing packages are auto-fetched — no full TeX Live
> install is required.

### 2. (Optional) VS Code + extension

VS Code is recommended for editing `.tex` files:

- Install the **LaTeX Workshop** extension (`James-Yu.latex-workshop`).
- Set its default recipe to use `tectonic` if you want to build with a keyboard shortcut.
  The simplest path is still to run `tectonic` in the terminal (see below).

---

## Editing & Building

### Build manually (terminal)

After editing a `.tex` file, run:

```bash
tectonic main.tex      # produces main.pdf
tectonic master.tex    # produces master.pdf
```

The PDF is written to the repo root, overwriting the previous one.

### Build on save (VS Code)

If LaTeX Workshop is configured to use Tectonic, `Cmd+S` will rebuild and live-preview the
PDF inside the editor.

### View the PDF

```bash
open main.pdf
```

---

## Repo structure

```
.
├── main.tex        # Main CV (2 pages) — edit content here
├── master.tex      # Master CV (2 pages, full keyword set)
├── main.pdf        # Generated PDF (kept in repo)
├── master.pdf      # Generated PDF (kept in repo)
├── main.log        # Build log (auto-generated, gitignored)
└── README.md       # This file
```

---

## CV sections

Both `main.tex` and `master.tex` follow the **MTecknology's Resume** layout:

- **Header**: name (bold, black), "Product Data Engineer" title, contact (phone / email / LinkedIn / GitHub).
- **Summary**: a 3–4 line positioning statement (product data engineer, not just an ETL builder).
- **Technical Skills**: 6 lines grouped by Languages / Data Engineering / Databases / BI & AI / Cloud & Integration / Stakeholder Communication.
- **Experience** (page 1): Jemmia Diamond, BabyBet, AVLI Group JSC, AISIA Research Lab.
- **Personal Projects** (page 2): 4 placeholder entries — replace with real projects.
- **Education** (page 2, bottom): UIT, Bachelor of Data Science, GPA 8.5/10 | IELTS 7.0.

---

## Important notes when editing

- **Keep it to 2 pages**: the layout has been carefully tuned so `main.tex` fits exactly
  2 pages. Adding content that overflows to page 3 means you must trim another bullet or
  reduce spacing (see `\vspace`, `itemsep`, `titlespacing` in the preamble).
- **Font**: uses `\usepackage{libertinus}`. Do **not** stack `charter` or `CormorantGaramond`
  on top — it breaks bold rendering (see git history to avoid repeating this bug).
- **Colors**: green `#0e6e55` (section titles), gold `#a16f0b` (rule line). Change them in
  `\definecolor{accentTitle}{HTML}{...}` in the preamble.
- **Job heading format**: `\headingBf{Title | Company}{Date}` (everything bold).
- **Page break**: a `\newpage` before *Personal Projects* forces content onto page 2.
- **Company names**: verified officially (Jemmia Diamond, AVLI Group JSC, AISIA Research Lab).
  If any is wrong, fix it and document it here.
- **AISIA vs ASIA**: currently "AISIA Research Lab" — confirm the lab's real name.

---

## Packaging / Submitting

Build the final PDF and send `main.pdf`:

```bash
tectonic main.tex && open main.pdf
```

Rename it before sending:

```bash
cp main.pdf "Tran_Nhat_Nam_Senior_Data_Engineer.pdf"
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Font shape TU/bch/b/n undefined` | `charter` stacked on top | Remove `\usepackage{charter}`, keep only `libertinus` |
| `Illegal unit of measure (pt inserted)` | `itemsep=-2px` | Change `px` to `pt` |
| `! Undefined control sequence \pdfglyphtounicode` | Running under XeTeX | Already handled via `\newcommand{\pdfglyphtounicode}[2]{}` |
| Overflows to page 3 | Added content | Trim a bullet or reduce `\vspace` / `titlespacing` |
| No bold text | Wrong font setup | Use exactly one font package (`libertinus`) |

---

## Credits

- Base layout: **MTecknology's Resume** (Michael Lustfield).
- CV content: Tran Nhat Nam.
