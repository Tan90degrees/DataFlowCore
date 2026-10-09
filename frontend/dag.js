export function template(version = "0.2.0") {
  return {
    name: "文本词频统计",
    input_path: "/dataflow/input.txt",
    pool: "default",
    runtime_version: version,
    dag_workers: 2,
    cpu: 1,
    memory_mb: 256,
    max_attempts: 3,
    retry_delay: 2,
    timeout: 3600,
    parameters: {},
    steps: [
      {
        id: "read",
        callable: "dataflowcore.operators:read_text",
        depends_on: [],
        parameters: {},
      },
      {
        id: "count",
        callable: "dataflowcore.operators:word_count",
        depends_on: ["read"],
        parameters: {},
      },
      {
        id: "write",
        callable: "dataflowcore.operators:write_json",
        depends_on: ["count"],
        parameters: {},
      },
    ],
  };
}

export function layers(steps) {
  const resolved = new Set(),
    result = [];
  while (resolved.size < steps.length) {
    const ready = steps.filter(
      (s) =>
        !resolved.has(s.id) &&
        (s.depends_on || []).every((d) => resolved.has(d)),
    );
    if (!ready.length) return [steps];
    result.push(ready);
    ready.forEach((s) => resolved.add(s.id));
  }
  return result;
}

const SVG = "http://www.w3.org/2000/svg";
function svg(tag, attrs = {}, text) {
  const node = document.createElementNS(SVG, tag);
  Object.entries(attrs).forEach(([key, value]) =>
    node.setAttribute(key, value),
  );
  if (text !== undefined) node.textContent = text;
  return node;
}

// DOM text nodes only: operator names and imported JSON are untrusted input.
export function graph(steps, { selected, progress = {}, onSelect } = {}) {
  const root = document.createElement("div");
  root.className = "graph-scroll";
  const visible = steps.slice(0, 200),
    groups = layers(visible),
    positions = new Map();
  groups.forEach((group, col) =>
    group.forEach((step, row) =>
      positions.set(step.id, { x: 24 + col * 240, y: 24 + row * 106 }),
    ),
  );
  const width = Math.max(480, groups.length * 240 + 24),
    height = Math.max(160, ...groups.map((g) => g.length * 106 + 28));
  const drawing = svg("svg", {
    width,
    height,
    viewBox: `0 0 ${width} ${height}`,
    role: "img",
    "aria-label": "DAG 依赖图",
  });
  const defs = svg("defs"),
    marker = svg("marker", {
      id: "arrow",
      viewBox: "0 0 10 10",
      refX: 9,
      refY: 5,
      markerWidth: 5,
      markerHeight: 5,
      orient: "auto-start-reverse",
    });
  marker.append(svg("path", { d: "M 0 0 L 10 5 L 0 10 z", fill: "#a3b2c8" }));
  defs.append(marker);
  drawing.append(defs);
  for (const step of visible) {
    const to = positions.get(step.id);
    for (const dep of step.depends_on || []) {
      const from = positions.get(dep);
      if (!from) continue;
      drawing.append(
        svg("path", {
          d: `M${from.x + 196},${from.y + 35} C${from.x + 219},${from.y + 35} ${to.x - 23},${to.y + 35} ${to.x},${to.y + 35}`,
          fill: "none",
          stroke: "#a3b2c8",
          "stroke-width": 1.5,
          "marker-end": "url(#arrow)",
        }),
      );
    }
  }
  for (const step of visible) {
    const p = positions.get(step.id),
      state = progress[step.id]?.state || "PENDING";
    const node = svg("g", {
      transform: `translate(${p.x},${p.y})`,
      class: `graph-node ${state} ${selected === step.id ? "selected" : ""}`,
      "data-step": step.id,
    });
    node.append(svg("rect", { width: 196, height: 72, rx: 10 }));
    node.append(svg("circle", { cx: 17, cy: 22, r: 4 }));
    node.append(
      svg("text", { x: 30, y: 27, class: "node-name" }, step.id.slice(0, 22)),
    );
    node.append(
      svg(
        "text",
        { x: 14, y: 52, class: "node-callable" },
        step.callable.split(":").at(-1).slice(0, 25),
      ),
    );
    node.append(svg("title", {}, `${step.id}\n${step.callable}\n${state}`));
    if (onSelect) {
      node.setAttribute("role", "button");
      node.setAttribute("tabindex", "0");
      node.setAttribute("aria-label", `编辑节点 ${step.id}`);
      node.addEventListener("click", () => onSelect(step.id));
      node.addEventListener("keydown", (e) => {
        if (["Enter", " "].includes(e.key)) {
          e.preventDefault();
          onSelect(step.id);
        }
      });
    }
    drawing.append(node);
  }
  root.append(drawing);
  if (steps.length > visible.length) {
    const note = document.createElement("p");
    note.className = "hint";
    note.textContent =
      "图预览前 200 个节点，JSON 导入/导出及 API 校验包含所有节点。";
    root.append(note);
  }
  return root;
}
