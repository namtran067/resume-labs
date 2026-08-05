# CV — Tran Nhat Nam (multi-CV workspace)

LaTeX-based CV (Harvard/ATS-friendly) compiled with **Tectonic**. Each CV lives in its own
**self-contained folder**: a thin `resume.tex` assembly plus a private set of content blocks
(preamble, header, summary, skills, experience, projects, education). Nothing is shared
between CVs — creating a new CV = copying a folder.

---

## Repo structure

```
.
├── template/                     # Starter skeleton — copy this to create a new CV
│   ├── resume.tex                #   assembly (inputs the blocks below)
│   ├── preamble.tex              #   layout: packages, colors, custom commands
│   ├── header.tex                #   name + title + contact   (placeholders)
│   ├── summary.tex               #   summary                  (placeholder)
│   ├── skills.tex                #   technical skills         (placeholder)
│   ├── education.tex             #   education                (placeholder)
│   ├── experience/company-a.tex  #   one file per job         (sample)
│   └── projects/project-a.tex    #   one file per project     (sample)
├── cv/                           # One folder per CV
│   └── data-engineer-jemmia/     # Senior Data Engineer @ Jemmia Diamond
│       ├── resume.tex            #   assembly
│       ├── preamble.tex
│       ├── header.tex
│       ├── summary.tex
│       ├── skills.tex
│       ├── experience/
│       │   ├── jemmia-diamond.tex
│       │   ├── babybet.tex
│       │   ├── avli-group.tex
│       │   └── aisia-research.tex
│       ├── projects/
│       │   ├── data-platform.tex
│       │   ├── rag-assistant.tex
│       │   ├── price-tracker.tex
│       │   └── asr-experiment.tex
│       ├── education.tex
│       └── resume.pdf            # built output
└── README.md
```

### Why one folder per CV?

Each application is a **frozen snapshot**. A CV folder is 100% self-contained and portable:
copy it anywhere, build it on its own, tweak its layout — it never breaks another CV. Blocks
are split into small files for easy editing, but they belong to **that CV only** (no shared
`content/` pool across CVs).

---

## Requirements

- **macOS** (tested on Apple Silicon) + **Homebrew**.
- The **Libertinus** font is fetched automatically by Tectonic — no manual install.

```bash
brew install tectonic
tectonic --version   # 0.17.0 or newer
```

---

## Build a CV

From the repo root — no extra flags needed (all blocks live next to `resume.tex`):

```bash
tectonic cv/data-engineer-jemmia/resume.tex
open cv/data-engineer-jemmia/resume.pdf
```

The PDF is written next to `resume.tex`, overwriting the previous one.

---

## Create a new CV

```bash
# 1. copy the skeleton
cp -r template cv/data-engineer-acme

# 2. edit the blocks (start with header.tex + summary.tex tailored to the JD)
#    add one experience/<company>.tex per job, one projects/<project>.tex per project

# 3. build
tectonic cv/data-engineer-acme/resume.tex
```

Folder naming convention: **`cv/<role>-<company>/`** (e.g. `cv/data-engineer-acme`,
`cv/data-analyst-fpt`).

> Tip: to tailor an existing CV for a new company with the **same role**, copy that CV folder
> instead of `template/` — you keep all the real experience/project blocks and only rewrite
> `summary.tex` (and maybe a couple of `skills.tex` lines) to match the JD.
> ```bash
> cp -r cv/data-engineer-jemmia cv/data-engineer-acme
> ```

---

## Editing blocks

- Each block file is self-contained LaTeX (no `\documentclass`, no `\begin{document}`).
- `resume.tex` only `\input`s `preamble.tex` then the blocks, in order.
- `preamble.tex` holds the layout: packages, colors, geometry, and custom commands:
  - `\documentTitle`, `\heading`/`\headingBf`/`\headingIt`, `\tinysection`,
    the `resume_list` environment, `\itemTitle`.
  - Colors: `accentTitle`/`accentText` (`#0e6e55`), `accentLine` (`#a16f0b`).
- **Keep each CV to 2 pages.** Overflow → trim a bullet or reduce `\vspace` in `resume.tex`.

### Notes

- **Font:** `\usepackage{libertinus}` only. Do **not** stack `charter` / `CormorantGaramond`
  on top — it breaks bold rendering (see git history).
- **Page break:** a `\newpage` in `resume.tex` forces content onto page 2.
- **Company names:** verified officially (Jemmia Diamond, AVLI Group JSC, AISIA Research Lab).

---

## Packaging / Submitting

```bash
tectonic cv/data-engineer-jemmia/resume.tex
cp cv/data-engineer-jemmia/resume.pdf "Tran_Nhat_Nam_Senior_Data_Engineer.pdf"
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `File 'preamble.tex' not found` | Building from inside the folder, or wrong CWD | Run `tectonic cv/<name>/resume.tex` from the repo root (paths are local to the folder) |
| `Font shape TU/bch/b/n undefined` | `charter` stacked on top | Keep only `libertinus` in `preamble.tex` |
| `Illegal unit of measure (pt inserted)` | `itemsep=-2px` | Use `pt`, not `px` |
| Overflows to page 3 | Too much content | Trim a bullet or reduce `\vspace` in `resume.tex` |

---

## Credits

- Base layout: **MTecknology's Resume** (Michael Lustfield).
- CV content: Tran Nhat Nam.
