import {
  flexRender,
  type Row,
  type RowData,
  type SortingState,
  useTable,
} from "@tanstack/react-table";
import { ArrowDown, ArrowUp, ChevronsUpDown } from "lucide-react";
import { memo, type ReactNode, useState } from "react";
import { cn } from "@/lib/cn";
import { type DataTableColumn, dataTableFeatures } from "./data-table-features";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./table";

interface DataTableProps<TData extends RowData> {
  columns: DataTableColumn<TData>[];
  data: TData[];
  empty?: ReactNode;
  onRowClick?: (row: TData) => void;
  rowKey?: (row: TData, index: number) => string;
  className?: string;
  initialSorting?: SortingState;
}

function DataTableImpl<TData extends RowData>({
  columns,
  data,
  empty,
  onRowClick,
  rowKey,
  className,
  initialSorting = [],
}: DataTableProps<TData>) {
  const [sorting, setSorting] = useState<SortingState>(initialSorting);

  const table = useTable({
    features: dataTableFeatures,
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
  });

  return (
    <div
      className={cn(
        "overflow-hidden rounded-[var(--card-radius)] border border-[var(--border)] bg-[var(--surface)] shadow-[var(--card-shadow)]",
        className,
      )}
    >
      <Table>
        <TableHeader>
          {table.getHeaderGroups().map((group) => (
            <TableRow key={group.id}>
              {group.headers.map((header) => {
                const canSort = header.column.getCanSort();
                const sorted = header.column.getIsSorted();
                return (
                  <TableHead key={header.id} style={{ width: header.getSize() }}>
                    {header.isPlaceholder ? null : canSort ? (
                      <button
                        type="button"
                        onClick={header.column.getToggleSortingHandler()}
                        className="inline-flex items-center gap-1 transition-colors hover:text-[var(--fg)]"
                      >
                        {flexRender(header.column.columnDef.header, header.getContext())}
                        {sorted === "asc" ? (
                          <ArrowUp className="size-3" aria-hidden />
                        ) : sorted === "desc" ? (
                          <ArrowDown className="size-3" aria-hidden />
                        ) : (
                          <ChevronsUpDown className="size-3 opacity-40" aria-hidden />
                        )}
                      </button>
                    ) : (
                      flexRender(header.column.columnDef.header, header.getContext())
                    )}
                  </TableHead>
                );
              })}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {table.getRowModel().rows.length === 0 ? (
            <TableRow>
              <TableCell
                colSpan={columns.length}
                className="h-32 text-center text-sm text-[var(--fg-subtle)]"
              >
                {empty ?? "No data"}
              </TableCell>
            </TableRow>
          ) : (
            table.getRowModel().rows.map((row: Row<typeof dataTableFeatures, TData>, index) => (
              <TableRow
                key={rowKey ? rowKey(row.original, index) : row.id}
                className={cn(
                  onRowClick &&
                    "cursor-pointer focus-visible:outline-none focus-visible:bg-[var(--surface-2)]",
                )}
                tabIndex={onRowClick ? 0 : undefined}
                onClick={onRowClick ? () => onRowClick(row.original) : undefined}
                onKeyDown={
                  onRowClick
                    ? (event) => {
                        if (event.key === "Enter" || event.key === " ") {
                          event.preventDefault();
                          onRowClick(row.original);
                        }
                      }
                    : undefined
                }
              >
                {row.getAllCells().map((cell) => (
                  <TableCell key={cell.id}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>
    </div>
  );
}

// Memoized so unrelated parent re-renders (e.g. the polling clock) don't
// re-render the whole table. The cast preserves the generic call signature
// that React.memo otherwise erases. Relies on callers passing stable
// `columns`/`onRowClick` (via useMemo/useCallback) to actually skip renders.
export const DataTable = memo(DataTableImpl) as typeof DataTableImpl;
