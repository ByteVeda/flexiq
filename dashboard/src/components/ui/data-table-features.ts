import {
  type ColumnDef,
  columnSizingFeature,
  createSortedRowModel,
  type RowData,
  rowSortingFeature,
  tableFeatures,
} from "@tanstack/react-table";

// Table v9 is opt-in per feature: only what is listed here ships in the bundle,
// and the feature set is baked into every column and table type.
export const dataTableFeatures = tableFeatures({
  columnSizingFeature,
  rowSortingFeature,
  sortedRowModel: createSortedRowModel(),
});

/** Column definition bound to the feature set `DataTable` registers. */
export type DataTableColumn<TData extends RowData> = ColumnDef<typeof dataTableFeatures, TData>;
