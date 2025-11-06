const statementsContainer = document.getElementById("statements-container");
const addStatementBtn = document.getElementById("add-statement");
const runButton = document.getElementById("run-button");
const requestStatus = document.getElementById("request-status");
const existingTablesInput = document.getElementById("existing-tables");

const liveTablesList = document.getElementById("live-tables");
const ephemeralTablesList = document.getElementById("ephemeral-tables");
const edgesTableBody = document.getElementById("edges-table");
const warningsList = document.getElementById("warnings");
const graphContainer = document.getElementById("graph-container");
const graphPlaceholder = document.getElementById("graph-placeholder");

let sigmaRenderer = null;

function createStatementCard(initialSql = "", enabled = true) {
  const template = document.getElementById("statement-template");
  const fragment = template.content.cloneNode(true);
  const card = fragment.querySelector(".statement-card");
  const textarea = card.querySelector(".statement-sql");
  const enabledInput = card.querySelector(".statement-enabled");
  const removeBtn = card.querySelector(".remove-statement");

  textarea.value = initialSql.trim();
  enabledInput.checked = enabled;

  removeBtn.addEventListener("click", () => {
    card.remove();
    refreshStatementIndices();
  });

  statementsContainer.appendChild(card);
  refreshStatementIndices();

  return card;
}

function refreshStatementIndices() {
  const cards = statementsContainer.querySelectorAll(".statement-card");
  cards.forEach((card, index) => {
    const badge = card.querySelector(".statement-index");
    badge.textContent = index + 1;
  });

  if (cards.length === 0) {
    requestStatus.classList.add("hidden");
  }
}

function parseTables(text) {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function collectStatements() {
  const cards = Array.from(
    statementsContainer.querySelectorAll(".statement-card")
  );
  return cards.map((card) => {
    const textarea = card.querySelector(".statement-sql");
    const enabledInput = card.querySelector(".statement-enabled");
    return {
      sql: textarea.value.trim(),
      enabled: enabledInput.checked,
    };
  });
}

function buildApiEndpoints() {
  const { protocol, hostname } = window.location;
  const endpoints = [];
  const defaultPaths = ["/api/lineage", "/"];

  defaultPaths.forEach((path) => {
    endpoints.push(`${window.location.origin}${path}`);
  });

  if (hostname === "localhost" || hostname === "127.0.0.1") {
    const alternateOrigins = [
      `${protocol}//127.0.0.1:5000`,
      `${protocol}//localhost:5000`,
    ];

    alternateOrigins.forEach((origin) => {
      defaultPaths.forEach((path) => {
        const url = `${origin}${path}`;
        if (!endpoints.includes(url)) {
          endpoints.push(url);
        }
      });
    });
  }

  return endpoints;
}

async function callLineageAPI(payload) {
  const endpoints = buildApiEndpoints();
  let lastError = null;

  for (const endpoint of endpoints) {
    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} ${response.statusText}: ${text}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("Failed to reach /api/lineage");
}

function updateStatus(message, isError = false) {
  requestStatus.textContent = message;
  requestStatus.classList.remove("hidden");
  if (isError) {
    requestStatus.classList.remove("text-slate-400");
    requestStatus.classList.add("text-red-400");
  } else {
    requestStatus.classList.add("text-slate-400");
    requestStatus.classList.remove("text-red-400");
  }
}

function renderList(listElement, values) {
  listElement.innerHTML = "";
  if (!values || values.length === 0) {
    const li = document.createElement("li");
    li.textContent = "None";
    li.className = "text-slate-500";
    listElement.appendChild(li);
    return;
  }

  values.forEach((value) => {
    const li = document.createElement("li");
    li.textContent = value;
    li.className =
      "px-3 py-1 rounded-full bg-slate-800 text-slate-200 inline-flex items-center";
    listElement.appendChild(li);
  });
}

function renderWarnings(warnings) {
  warningsList.innerHTML = "";
  const entries = Object.entries(warnings || {});
  if (entries.length === 0) {
    const li = document.createElement("li");
    li.textContent = "No warnings";
    li.className = "text-slate-500";
    warningsList.appendChild(li);
    return;
  }

  entries.forEach(([key, values]) => {
    const li = document.createElement("li");
    li.innerHTML = `<span class="text-slate-400">Statement ${
      Number(key) + 1
    }:</span> ${values.join(", ")}`;
    li.className = "px-3 py-2 rounded-xl bg-slate-800 text-slate-200";
    warningsList.appendChild(li);
  });
}

function renderEdgesTable(edges) {
  edgesTableBody.innerHTML = "";
  if (!edges || edges.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 4;
    td.textContent = "No lineage edges returned";
    td.className = "py-4 text-center text-slate-500";
    tr.appendChild(td);
    edgesTableBody.appendChild(tr);
    return;
  }

  edges.forEach((edge) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="py-2 pr-4 text-slate-200">${edge.src}</td>
      <td class="py-2 pr-4 text-slate-400">${edge.op}</td>
      <td class="py-2 pr-4 text-slate-200">${edge.dst}</td>
      <td class="py-2 text-slate-400">${
        edge.stmt_index != null ? edge.stmt_index + 1 : "—"
      }</td>
    `;
    edgesTableBody.appendChild(tr);
  });
}

async function renderGraph(data) {
  const detailedEdges = data?.detailed_edges || [];
  const simpleEdges = (data?.edges || []).map(([src, dst]) => ({
    src,
    dst,
    op: "FLOW",
  }));
  const edges = detailedEdges.length > 0 ? detailedEdges : simpleEdges;

  if (!edges.length) {
    if (sigmaRenderer) {
      sigmaRenderer.kill();
      sigmaRenderer = null;
    }
    graphPlaceholder.classList.remove("hidden");
    graphPlaceholder.textContent = "No edges to visualize.";
    graphContainer.style.background = "#020617";
    return;
  }

  const Graph = window.graphology.Graph;
  const graph = new Graph({ allowSelfLoops: false });
  const nodes = new Set();
  edges.forEach((edge) => {
    nodes.add(edge.src);
    nodes.add(edge.dst);
  });

  nodes.forEach((name) => {
    if (!graph.hasNode(name)) {
      graph.addNode(name, {
        label: name,
        size: 12,
        color: "#6366f1",
      });
    }
  });

  edges.forEach((edge, index) => {
    const key = `${edge.src}->${edge.dst}-${index}`;
    if (!graph.hasEdge(key)) {
      graph.addDirectedEdgeWithKey(key, edge.src, edge.dst, {
        label: edge.op,
        color: "#38bdf8",
        size: 2,
      });
    }
  });

  const elk = new window.ELK();
  const elkGraph = {
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "RIGHT",
      "elk.layered.spacing.nodeNodeBetweenLayers": "80",
      "elk.spacing.nodeNode": "40",
    },
    children: Array.from(nodes).map((nodeId) => ({
      id: nodeId,
      width: Math.max(140, nodeId.length * 8),
      height: 50,
      labels: [{ text: nodeId }],
    })),
    edges: edges.map((edge, index) => ({
      id: `e${index}`,
      sources: [edge.src],
      targets: [edge.dst],
      labels: edge.op ? [{ text: edge.op }] : undefined,
    })),
  };

  const layout = await elk.layout(elkGraph);
  const positions = new Map();
  layout.children?.forEach((child) => {
    positions.set(child.id, {
      x: child.x || 0,
      y: child.y || 0,
    });
  });

  positions.forEach((pos, nodeId) => {
    if (graph.hasNode(nodeId)) {
      graph.mergeNodeAttributes(nodeId, {
        x: pos.x,
        y: pos.y,
      });
    }
  });

  if (sigmaRenderer) {
    sigmaRenderer.kill();
    sigmaRenderer = null;
  }

  graphPlaceholder.classList.add("hidden");
  graphContainer.style.background = "#020617";
  // pick the Sigma constructor regardless of how the UMD was bundled
  const SigmaCtor =
    window.Sigma || // common UMD global
    window.sigma?.Sigma || // sometimes nested
    window.sigma?.default; // sometimes default-exported

  if (typeof SigmaCtor !== "function") {
    console.error("Sigma globals", {
      Sigma: window.Sigma,
      sigma: window.sigma,
    });
    throw new Error(
      "Sigma constructor not found. Check the <script> tag and load order."
    );
  }

  sigmaRenderer = new SigmaCtor(graph, graphContainer, {
    renderEdgeLabels: true,
    minCameraRatio: 0.1,
    maxCameraRatio: 2,
  });
}

function validateInput(existingTables, statements) {
  const errors = [];
  if (!statements.length) {
    errors.push("Please provide at least one statement.");
  }
  statements.forEach((stmt, idx) => {
    if (!stmt.sql) {
      errors.push(`Statement #${idx + 1} is empty.`);
    }
  });
  return errors;
}

async function handleRun() {
  const tables = parseTables(existingTablesInput.value);
  const statements = collectStatements();
  const errors = validateInput(tables, statements);

  if (errors.length) {
    updateStatus(errors.join(" "), true);
    return;
  }

  updateStatus("Sending request...", false);

  const payload = {
    initial_catalog: { tables },
    statements: statements.map((stmt) => ({
      sql: stmt.sql,
      enabled: stmt.enabled,
    })),
  };

  try {
    const response = await callLineageAPI(payload);
    updateStatus("Lineage analyzed successfully.");
    renderList(liveTablesList, response.live_tables);
    renderList(ephemeralTablesList, response.ephemeral_tables);
    renderEdgesTable(response.detailed_edges);
    renderWarnings(response.warnings);
    renderGraph(response);
  } catch (error) {
    console.error(error);
    updateStatus(`Request failed: ${error.message}`, true);
    if (sigmaRenderer) {
      sigmaRenderer.kill();
      sigmaRenderer = null;
    }
    graphPlaceholder.classList.remove("hidden");
    graphPlaceholder.textContent = "Unable to load lineage.";
    graphContainer.style.background = "#0f172a";
  }
}

function bootstrapExamples() {
  existingTablesInput.value = "public.sales\npublic.customers";

  const sampleStatements = [
    `CREATE TABLE public.temp_sales AS
SELECT * FROM public.sales WHERE sale_date >= '2024-01-01';`,
    `CREATE TABLE public.high_value_sales AS
SELECT s.*
FROM public.temp_sales s
JOIN public.customers c ON s.customer_id = c.id
WHERE c.lifetime_value > 10000;`,
  ];

  sampleStatements.forEach((sql) => createStatementCard(sql));
}

addStatementBtn.addEventListener("click", () => createStatementCard());
runButton.addEventListener("click", handleRun);

bootstrapExamples();
