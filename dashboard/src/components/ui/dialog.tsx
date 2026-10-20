import * as DialogPrimitive from "@radix-ui/react-dialog";
import { X } from "lucide-react";
import { type ComponentPropsWithoutRef, forwardRef, type HTMLAttributes } from "react";
import { cn } from "@/lib/cn";

const Dialog = DialogPrimitive.Root;
const DialogTrigger = DialogPrimitive.Trigger;
const DialogPortal = DialogPrimitive.Portal;
const DialogClose = DialogPrimitive.Close;

const DialogOverlay = forwardRef<
  HTMLDivElement,
  ComponentPropsWithoutRef<typeof DialogPrimitive.Overlay>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Overlay
    ref={ref}
    className={cn(
      "fixed inset-0 z-50 bg-black/50 backdrop-blur-[2px] data-[state=open]:animate-fade-in",
      className,
    )}
    {...props}
  />
));
DialogOverlay.displayName = DialogPrimitive.Overlay.displayName;

const DialogContent = forwardRef<
  HTMLDivElement,
  ComponentPropsWithoutRef<typeof DialogPrimitive.Content>
>(({ className, children, ...props }, ref) => (
  <DialogPortal>
    <DialogOverlay />
    <DialogPrimitive.Content
      ref={ref}
      className={cn(
        "fixed left-1/2 top-1/2 z-50 w-full max-w-lg -translate-x-1/2 -translate-y-1/2 rounded-lg border border-[var(--border-strong)] bg-[var(--surface)] p-6 shadow-2xl duration-200",
        // Centred by translate, so a dialog taller than the viewport would be
        // clipped at both ends with no way to reach them: cap it and scroll.
        "max-h-[calc(100dvh-2rem)] overflow-y-auto overscroll-contain",
        "data-[state=open]:animate-slide-up",
        className,
      )}
      {...props}
    >
      {children}
      <DialogPrimitive.Close
        aria-label="Close"
        className="absolute right-4 top-4 rounded-md p-1 text-[var(--fg-subtle)] opacity-70 transition-opacity hover:opacity-100 focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-ring)]"
      >
        <X className="size-4" />
      </DialogPrimitive.Close>
    </DialogPrimitive.Content>
  </DialogPortal>
));
DialogContent.displayName = DialogPrimitive.Content.displayName;

function DialogHeader({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn("flex flex-col gap-1.5 text-center sm:text-left", className)} {...props} />
  );
}

function DialogFooter({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("mt-6 flex flex-col-reverse gap-2 sm:flex-row sm:justify-end", className)}
      {...props}
    />
  );
}

const DialogTitle = forwardRef<
  HTMLHeadingElement,
  ComponentPropsWithoutRef<typeof DialogPrimitive.Title>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Title
    ref={ref}
    className={cn("text-base font-semibold tracking-tight", className)}
    {...props}
  />
));
DialogTitle.displayName = DialogPrimitive.Title.displayName;

const DialogDescription = forwardRef<
  HTMLParagraphElement,
  ComponentPropsWithoutRef<typeof DialogPrimitive.Description>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Description
    ref={ref}
    className={cn("text-sm text-[var(--fg-muted)]", className)}
    {...props}
  />
));
DialogDescription.displayName = DialogPrimitive.Description.displayName;

export {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogOverlay,
  DialogPortal,
  DialogTitle,
  DialogTrigger,
};
