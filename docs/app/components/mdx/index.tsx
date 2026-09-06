import type { MDXComponents } from "mdx/types";
import type { ComponentProps } from "react";
import { Link } from "react-router";
import * as Demos from "@/components/demos/mdx";
import * as Diagrams from "@/components/diagrams";
import { SdkBinding, SdkLang, SdkName, SdkSwap } from "@/components/sdk-text";
import { Callout } from "./callout";
import { Card, Cards } from "./card";
import { CodeBlock } from "./code-block";
import { CodeTabs, SdkLink, SdkOnly } from "./sdk";
import { MdxTable } from "./table";
import { Tab, Tabs } from "./tabs";

// Internal links use the router for client navigation; external/anchor links
// stay plain. Code blocks are highlighted at build time by rehype-shiki, and
// headings/paragraphs are styled by element selectors inside `.article`.
function Anchor({ href, children, ...rest }: ComponentProps<"a">) {
  if (href?.startsWith("/")) {
    return (
      <Link to={href} {...rest}>
        {children}
      </Link>
    );
  }
  const external = href?.startsWith("http");
  return (
    <a
      href={href}
      {...(external ? { target: "_blank", rel: "noreferrer" } : {})}
      {...rest}
    >
      {children}
    </a>
  );
}

/** Components made available to every MDX page via `<MDXProvider>`. */
export const mdxComponents: MDXComponents = {
  a: Anchor,
  pre: CodeBlock,
  table: MdxTable,
  Callout,
  Tabs,
  Tab,
  CodeTabs,
  SdkOnly,
  SdkLink,
  SdkName,
  SdkLang,
  SdkBinding,
  SdkSwap,
  Card,
  Cards,
  ...Diagrams,
  ...Demos,
};
