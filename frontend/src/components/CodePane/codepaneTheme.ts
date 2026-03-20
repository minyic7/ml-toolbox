export const CODEPANE_THEME_NAME = "codepane-dark";

export const codepaneTheme = {
  base: "vs-dark",
  inherit: true,
  rules: [
    // Keywords: purple
    { token: "keyword", foreground: "C084FC" },
    { token: "keyword.control", foreground: "C084FC" },
    { token: "keyword.operator", foreground: "C084FC" },

    // Functions: amber
    { token: "support.function", foreground: "FBBF24" },
    { token: "entity.name.function", foreground: "FBBF24" },

    // Strings: green
    { token: "string", foreground: "86EFAC" },
    { token: "string.escape", foreground: "86EFAC" },

    // Comments: muted purple
    { token: "comment", foreground: "4D4660" },
    { token: "comment.block", foreground: "4D4660" },
    { token: "comment.line", foreground: "4D4660" },

    // Variables: light blue
    { token: "variable", foreground: "93C5FD" },
    { token: "variable.predefined", foreground: "93C5FD" },
    { token: "identifier", foreground: "93C5FD" },

    // Types / built-ins keep readable defaults
    { token: "type", foreground: "C084FC" },
    { token: "number", foreground: "FBBF24" },
    { token: "delimiter", foreground: "E2E0F0" },
  ],
  colors: {
    "editor.background": "#1A1625",
    "editor.foreground": "#E2E0F0",
    "editorLineNumber.foreground": "#3D3650",
    "editorLineNumber.activeForeground": "#4D4660",
    "editor.selectionBackground": "#2D264080",
    "editor.lineHighlightBackground": "#00000000",
    "editorCursor.foreground": "#C084FC",
    "editorWidget.background": "#1A1625",
    "editorWidget.border": "#2D2640",
    "input.background": "#211C2E",
    "input.border": "#2D2640",
    "dropdown.background": "#211C2E",
    "dropdown.border": "#2D2640",
    "list.hoverBackground": "#2D2640",
    "list.activeSelectionBackground": "#2D2640",
    "editorSuggestWidget.background": "#211C2E",
    "editorSuggestWidget.border": "#2D2640",
    "editorSuggestWidget.selectedBackground": "#2D2640",
    "scrollbarSlider.background": "#2D264060",
    "scrollbarSlider.hoverBackground": "#2D264090",
    "scrollbarSlider.activeBackground": "#2D2640",
  },
} as const;
