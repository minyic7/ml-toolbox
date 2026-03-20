interface ErrorTracebackProps {
  error: string;
}

export function ErrorTraceback({ error }: ErrorTracebackProps) {
  return (
    <div className="output-error-traceback">
      {error}
    </div>
  );
}
