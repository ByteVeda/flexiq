import { fileURLToPath } from "node:url";
import mdx from "@mdx-js/rollup";
import { reactRouter } from "@react-router/dev/vite";
import rehypeShiki from "@shikijs/rehype";
import tailwindcss from "@tailwindcss/vite";
import rehypeAutolinkHeadings from "rehype-autolink-headings";
import rehypeSlug from "rehype-slug";
import remarkFrontmatter from "remark-frontmatter";
import remarkGfm from "remark-gfm";
import remarkMdxFrontmatter from "remark-mdx-frontmatter";
import { defineConfig } from "vite";
import tsconfigPaths from "vite-tsconfig-paths";
import { docsManifest } from "./vite-plugin-docs-manifest";

// Deploy under /flexiq on GitHub Pages; serve from root locally.
const base = process.env.DOCS_BASE_PATH
  ? `${process.env.DOCS_BASE_PATH}/`
  : "/";

const mdxComponentDir = (name: string) =>
  fileURLToPath(new URL(`./app/components/mdx/${name}.tsx`, import.meta.url));

export default defineConfig({
  base,
  resolve: {
    alias: {
      // The reused content MDX imports Fumadocs components; map those paths to
      // our own design-matched shims so the content compiles unchanged.
      "fumadocs-ui/components/callout": mdxComponentDir("callout"),
      "fumadocs-ui/components/tabs": mdxComponentDir("tabs"),
      "fumadocs-ui/components/card": mdxComponentDir("card"),
    },
  },
  plugins: [
    tailwindcss(),
    docsManifest(),
    // MDX must transform `.mdx` before React Router's plugin processes routes.
    {
      enforce: "pre",
      ...mdx({
        remarkPlugins: [
          remarkGfm,
          remarkFrontmatter,
          [remarkMdxFrontmatter, { name: "frontmatter" }],
        ],
        rehypePlugins: [
          rehypeSlug,
          [
            rehypeShiki,
            {
              // github-dark-default, not github-dark: the latter's comment
              // colour (#6a737d) is 3.96:1 on the code panel. This one's
              // (#8b949e) is 6.20:1, and the rest of the palette is the same
              // family. github-light already clears 4.82:1 on its panel.
              themes: { light: "github-light", dark: "github-dark-default" },
              // Emit only --shiki-light/--shiki-dark CSS vars (no inline color/bg),
              // so app.css can switch them on our [data-theme] selector.
              defaultColor: false,
              // Tag each <pre> with its language so the CodeBlock wrapper can
              // render the design's header bar.
              transformers: [
                {
                  name: "code-language",
                  pre(
                    this: { options: { lang: string } },
                    node: { properties: Record<string, unknown> },
                  ) {
                    node.properties["data-language"] = this.options.lang;
                  },
                },
              ],
            },
          ],
          [rehypeAutolinkHeadings, { behavior: "wrap" }],
        ],
        providerImportSource: "@mdx-js/react",
      }),
    },
    reactRouter(),
    tsconfigPaths(),
  ],
});
