<script lang="ts">
  import Plotly from "plotly.js-dist";
  import { onMount } from "svelte";
  import { COLORS } from "../ts/util"; // <-- your palette

  // API types
  type BucketKey = string;
  type Stat = { count: number; disk: number; atime: number; mtime: number };
  type Users = Record<string, Record<BucketKey, Stat>>;
  type Row = { path: string; users: Users };

  // Props
  export let data: Row[];                  // your API payload
  export let title = "Disk usage • Folder → User";
  export let height = 620;

  const EPS = 1; // one-byte sliver so zero-byte users still render
  let el: HTMLDivElement;

  // ---------- utils ----------
  function humanBytes(bytes: number): string {
    if (!isFinite(bytes) || bytes <= 0) return "0 B";
    const u = ["B","KB","MB","GB","TB","PB"];
    let i = 0, v = bytes;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return `${v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v.toFixed(2)} ${u[i]}`;
  }

  // Map users -> COLORS (deterministic, cycles if more users than colors)
  function buildUserColors(users: string[], palette: string[] = COLORS): Map<string,string> {
    const m = new Map<string,string>();
    const sorted = [...users].sort();
    const n = Math.max(1, palette.length);
    sorted.forEach((u, i) => m.set(u, palette[i % n]));
    return m;
  }

  // ---------- aggregation ----------
  function aggregate(input: Row[]) {
    interface Agg { bytes: number; files: number; }
    const folderTotals = new Map<string, Agg>();              // path -> totals
    const folderUser = new Map<string, Map<string, Agg>>();   // path -> (user -> agg)
    const allUsers = new Set<string>();

    // collect totals + user set
    for (const row of input) {
      let ft = folderTotals.get(row.path);
      if (!ft) { ft = { bytes: 0, files: 0 }; folderTotals.set(row.path, ft); }

      let perUser = folderUser.get(row.path);
      if (!perUser) { perUser = new Map(); folderUser.set(row.path, perUser); }

      for (const [user, buckets] of Object.entries(row.users)) {
        allUsers.add(user);
        let ua = perUser.get(user);
        if (!ua) { ua = { bytes: 0, files: 0 }; perUser.set(user, ua); }
        for (const stat of Object.values(buckets)) {
          ua.bytes += stat.disk;
          ua.files += stat.count;
          ft.bytes += stat.disk;
          ft.files += stat.count;
        }
      }
    }

    const usersList = Array.from(allUsers).sort();
    const userColor = buildUserColors(usersList, COLORS);
    const folderPaths = Array.from(folderTotals.keys()).sort();

    // treemap arrays (folders are top-level)
    const labels: string[] = [];
    const ids: string[] = [];
    const parents: string[] = [];
    const values: number[] = [];
    const colors: string[] = [];
    const texts: string[] = [];
    // custom: [type, folderPath, user, sizeStr, files, isSliver]
    const custom: Array<[string, string, string, string, number, number]> = [];

    for (const path of folderPaths) {
      const fid = `f|${path}`;
      const perUser = folderUser.get(path) ?? new Map<string, Agg>();
      let childrenSum = 0;

      // Add ALL users under this folder (sliver if 0 B)
      for (const user of usersList) {
        const ua = perUser.get(user) ?? { bytes: 0, files: 0 };
        const isSliver = ua.bytes === 0 ? 1 : 0;
        const val = isSliver ? EPS : ua.bytes;
        childrenSum += val;

        labels.push(user);
        ids.push(`u|${path}|${user}`);
        parents.push(fid);
        values.push(val);
        colors.push(userColor.get(user)!);
        texts.push(humanBytes(ua.bytes));
        custom.push(["user", path, user, humanBytes(ua.bytes), ua.files, isSliver]);
      }

      // Folder parent (top-level)
      const ft = folderTotals.get(path)!;
      labels.push(path);
      ids.push(fid);
      parents.push("");
      values.push(childrenSum);                 // sum of children (keeps 'total' mode consistent)
      colors.push("#cbd5e1");                   // neutral folder color
      texts.push(humanBytes(ft.bytes));         // show real total as label text
      custom.push(["folder", path, "", humanBytes(ft.bytes), ft.files, 0]);
    }

    return { labels, ids, parents, values, colors, texts, custom };
  }

  onMount(() => {
    const { labels, ids, parents, values, colors, texts, custom } = aggregate(data);

    const trace: Partial<Plotly.PlotData> = {
      type: "treemap",
      branchvalues: "total",
      labels, ids, parents, values,
      marker: { colors },
      text: texts,
      textinfo: "label+text+percent parent",
      customdata: custom,
      hovertemplate:
        "<b>%{label}</b> <span style='opacity:0.6'>(%{customdata[0]})</span><br>" +
        "Folder: %{customdata[1]}<br>" +
        "User: %{customdata[2]}<br>" +
        "Size: %{customdata[3]}%{customdata[5]:+, sliver}<br>" +
        "Files: %{customdata[4]:,}<extra></extra>",
    };

    const layout: Partial<Plotly.Layout> = {
      title,
      height,
      margin: { t: 50, l: 25, r: 25, b: 25 },
      uniformtext: { mode: "hide", minsize: 10 },
    };

    Plotly.newPlot(el, [trace], layout, { responsive: true });

    const onResize = () => Plotly.Plots.resize(el);
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      Plotly.purge(el);
    };
  });
</script>

<div bind:this={el} style="width:100%; height:{height}px;"></div>
