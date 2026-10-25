# Colvenna Isle brand assets

**S3 上的持久化分析数据库** / **Durable analytics databases on S3**

The name, subtitle and usage rules come from the [Colvenna series index](https://github.com/colvenna/colvenna), checked on 2026-10-24. The series mark is three concentric teal flow lines forming a C, with the innermost line flowing toward a gold result point. Colvenna is the primary, bold wordmark; Isle uses regular weight. There are no duck or bay elements.

The mark geometry and palette follow the approved [series design, Issue #28](https://github.com/amliuyong/colvenna-for-clickhouse/issues/28) and its [Apache-2.0 generator](https://github.com/amliuyong/colvenna-for-clickhouse/blob/main/assets/brand/generate.py). This repository's [generator](../../scripts/render_brand.py) renders the assets and both consoles' inline marks from the same geometry.

![Light, dark and small-size brand preview](brand-preview.svg)

| Asset | Purpose |
| --- | --- |
| [icon.svg](icon.svg) | Square application mark; minimum 24px |
| [logo-light.svg](logo-light.svg), [logo-dark.svg](logo-dark.svg) | Wordmarks with the uppercase English subtitle; suggested minimum width 400px |
| [brand-preview.svg](brand-preview.svg) | Light/dark wordmarks and 96/64/32/24px icons |
| [logos.png](../design/evidence/logos.png) | Chromium render for visual review |
| [architecture.svg](architecture.svg) | Technical architecture; factual upstream references |
| [promo.gif](promo.gif), [promo.mp4](promo.mp4) | Product promotion: silent 960×540 GIF and narrated 1920×1080 MP4 |
| [promo-source](promo-source/README.md) | Editable video scenes, narration and regeneration instructions |

The promotional screens are illustrative mock-ups, and the result matches the [quickstart](../../examples/quickstart/README.md) output. Speech is AI-generated. Promotion makes no new runtime acceptance claim.

Keep the original proportions and leave at least one eighth of the icon's width as clear space. Use the icon for narrow placements. SVGs are self-contained; wordmarks use Arial, Helvetica and system sans-serif fallback.

| Element | Palette |
| --- | --- |
| Background | `#11556B` → `#062533` |
| Flow | `#1BB59E` → `#4FF2D6` |
| Result point | `#F5CB68` |
| Light/dark wordmark | `#123D4A` / `#EAF7F4` |

## Trademark check

On 2026-10-24, the [DuckDB trademark guidelines](https://duckdb.org/trademark_guidelines) were checked directly. They prohibit DuckDB or Duck as a product brand, modified or look-alike duck logos, and use of the marks in dashboards, menus and developer consoles without written approval. Domains containing duckdb require permission; promotional use has additional restrictions.

The requester chose **Colvenna Isle** without seeking Foundation approval ([Issue #19](https://github.com/colvenna/colvenna-isle/issues/19)). The name, subtitle, logos, consoles, press release and promotion use the independent brand. Technical documentation may factually describe its engine and includes attribution. No product domain containing duckdb is used. The 2.0.0 candidate also adopts Colvenna module, configuration, protocol and system-column names; see the [migration design](../design/identifier-migration.md).

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.

Colvenna legal clearance and trademark registration are `WAIVED_BY_REQUESTER_NOT_PASS`, per the requester's [decision](https://github.com/amliuyong/colvenna-for-clickhouse/issues/30). They have not passed and do not block this rename.

## Regeneration

```bash
python3 scripts/render_brand.py
python3 scripts/render_architecture.py
python3 scripts/check_docs.py
```

Original assets and the adapted Colvenna generator use the project's [Apache-2.0 license](../../LICENSE). See [third-party notices](../../THIRD_PARTY_NOTICES.md) for bundled video tooling.
