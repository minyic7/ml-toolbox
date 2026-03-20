import { describe, it, expect } from "vitest";
import { normalizeNodeParams } from "./api";

describe("normalizeNodeParams", () => {
  it("preserves valid ParamDefinition[] arrays", () => {
    const nodes = [
      {
        params: [
          {
            type: "select",
            name: "strategy",
            default: "mean",
            options: ["mean", "median"],
          },
        ],
      },
    ];
    normalizeNodeParams(nodes as any);
    expect(nodes[0].params).toEqual([
      {
        type: "select",
        name: "strategy",
        default: "mean",
        options: ["mean", "median"],
      },
    ]);
  });

  it("converts dict params to ParamDefinition[]", () => {
    const nodes = [{ params: { strategy: "mean", epochs: 100 } }];
    normalizeNodeParams(nodes as any);
    expect(Array.isArray(nodes[0].params)).toBe(true);
    expect(nodes[0].params).toHaveLength(2);
    expect(nodes[0].params).toContainEqual({
      type: "text",
      name: "strategy",
      default: "mean",
    });
    expect(nodes[0].params).toContainEqual({
      type: "text",
      name: "epochs",
      default: 100,
    });
  });

  it("handles null params", () => {
    const nodes = [{ params: null }];
    normalizeNodeParams(nodes as any);
    expect(nodes[0].params).toEqual([]);
  });

  it("handles undefined params", () => {
    const nodes = [{ params: undefined }];
    normalizeNodeParams(nodes as any);
    expect(nodes[0].params).toEqual([]);
  });

  it("handles empty dict params", () => {
    const nodes = [{ params: {} }];
    normalizeNodeParams(nodes as any);
    expect(nodes[0].params).toEqual([]);
  });

  it("handles empty array params", () => {
    const nodes = [{ params: [] }];
    normalizeNodeParams(nodes as any);
    expect(nodes[0].params).toEqual([]);
  });

  it("handles multiple nodes with mixed formats", () => {
    const nodes = [
      { params: [{ type: "text", name: "a", default: 1 }] },
      { params: { b: 2 } },
      { params: null },
    ];
    normalizeNodeParams(nodes as any);
    expect(Array.isArray(nodes[0].params)).toBe(true);
    expect(Array.isArray(nodes[1].params)).toBe(true);
    expect(nodes[1].params).toContainEqual({
      type: "text",
      name: "b",
      default: 2,
    });
    expect(nodes[2].params).toEqual([]);
  });
});
