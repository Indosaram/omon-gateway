import * as React from "react"
import { cn } from "../../lib/utils"

interface DialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  children: React.ReactNode
}

export function Dialog({ open, onOpenChange, children }: DialogProps) {
  if (!open) return null
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div
        className="fixed inset-0 bg-background/80 backdrop-blur-sm transition-opacity animate-in fade-in"
        onClick={() => onOpenChange(false)}
      />
      <div className="relative z-50 w-full max-w-lg rounded-lg border bg-card p-6 shadow-lg animate-in zoom-in-95">
        {children}
      </div>
    </div>
  )
}
