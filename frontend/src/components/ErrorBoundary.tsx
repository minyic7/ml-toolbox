import React from "react";

interface Props {
  children: React.ReactNode;
  /** Use "compact" for inline section-level boundaries (smaller fallback UI). */
  variant?: "full" | "compact";
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export default class ErrorBoundary extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error("ErrorBoundary caught an error:", error, info.componentStack);
  }

  render() {
    if (!this.state.hasError) {
      return this.props.children;
    }

    if (this.props.variant === "compact") {
      return (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            height: "100%",
            fontFamily: "system-ui, sans-serif",
            padding: "1.5rem",
            gap: "0.75rem",
          }}
        >
          <p style={{ fontSize: "0.9rem", color: "#888", margin: 0 }}>
            This section encountered an error
          </p>
          <button
            onClick={() => window.location.reload()}
            style={{
              padding: "0.35rem 1rem",
              fontSize: "0.85rem",
              borderRadius: "6px",
              border: "1px solid #ccc",
              background: "#fff",
              cursor: "pointer",
            }}
          >
            Reload
          </button>
        </div>
      );
    }

    return (
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          height: "100vh",
          fontFamily: "system-ui, sans-serif",
          padding: "2rem",
        }}
      >
        <h1 style={{ fontSize: "1.5rem", marginBottom: "1rem" }}>
          Something went wrong
        </h1>
        <button
          onClick={() => window.location.reload()}
          style={{
            padding: "0.5rem 1.25rem",
            fontSize: "1rem",
            borderRadius: "6px",
            border: "1px solid #ccc",
            background: "#fff",
            cursor: "pointer",
            marginBottom: "1.5rem",
          }}
        >
          Reload
        </button>
        <details style={{ maxWidth: "600px", width: "100%" }}>
          <summary style={{ cursor: "pointer", color: "#666" }}>
            Error details
          </summary>
          <pre
            style={{
              marginTop: "0.5rem",
              padding: "1rem",
              background: "#f5f5f5",
              borderRadius: "6px",
              overflow: "auto",
              fontSize: "0.85rem",
              whiteSpace: "pre-wrap",
            }}
          >
            {this.state.error?.message}
            {"\n\n"}
            {this.state.error?.stack}
          </pre>
        </details>
      </div>
    );
  }
}
