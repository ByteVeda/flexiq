declare module "virtual:docs-manifest" {
  export const DOCS: {
    slug: string;
    title: string;
    description: string;
    canonical?: string;
  }[];
}

declare module "virtual:docs-search-index" {
  /** Serialised MiniSearch index — JSON text for `MiniSearch.loadJSON`. */
  export const SEARCH_INDEX: string;
}

declare module "virtual:docs-corpus" {
  /** Page bodies for /llms-full.txt. Empty outside the SSR build (see the
   *  manifest plugin) — nothing in the client graph may depend on it. */
  export const CORPUS: {
    slug: string;
    title: string;
    text: string;
    code: string;
  }[];
}
