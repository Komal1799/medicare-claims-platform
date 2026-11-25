"""Parse the CMS CCW '30 Chronic Conditions Algorithms' PDF into a flat CSV.
Output: one row per (condition, ICD-10-CM code, role) with codes stored WITHOUT dots,
matching the CMS claims format. Run locally: python parse_ccw.py <pdf> <out.csv>
Requires: pdftotext (poppler-utils)."""
import re, csv, subprocess, sys

pdf, out = sys.argv[1], sys.argv[2]
text = subprocess.run(["pdftotext", "-layout", pdf, "-"], capture_output=True, text=True, check=True).stdout

# Keep only the algorithm table: from the first table header to the footnotes
start = text.index("Acute ")
end = text.index("ICD-10 codes are effective")
lines = text[start:end].splitlines()

CODE = re.compile(r"\b([A-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?)\b")
PERIOD = re.compile(r"^\s?(\S.{0,17}?)\s{2,}(\d) years?\b")
skip = ("Reference", "Algorithms", "(# of years)", "Claims to Qualify", "Chronic Conditions Warehouse", "MBSF_CHRONIC")

rows, cond, years, role, qualify = [], None, None, "include", []
def flush():
    pass

conditions = []
for line in lines:
    if not line.strip() or any(line.strip().startswith(s) for s in skip):
        continue
    m = PERIOD.match(line)
    name_part = line[:19].strip()
    body = line[19:]
    if m:  # a new condition starts on this line
        cond = {"name": [m.group(1).strip()], "years": int(m.group(2)), "qualify": []}
        conditions.append(cond)
        role = "include"
        body = line[m.end():]
    elif name_part and cond:
        cond["name"].append(name_part)
    # split codes column from the right-hand "claims to qualify" column (large gap)
    parts = re.split(r"\s{3,}", body.strip())
    codes_text = parts[0] if parts else ""
    if len(parts) > 1:
        cond["qualify"].append(" ".join(parts[1:]))
    elif not CODE.search(codes_text) and cond:  # pure qualify-text line
        cond["qualify"].append(codes_text)
        codes_text = ""
    if "EXCLUSION" in codes_text:
        before, after = codes_text.split("EXCLUSION", 1)
        for c in CODE.findall(before):
            cond.setdefault("codes", []).append((c, "include"))
        role = "exclude"
        codes_text = after
    for c in CODE.findall(codes_text):
        cond.setdefault("codes", []).append((c, role))

with open(out, "w", newline="") as f:
    w = csv.writer(f, delimiter="|")
    w.writerow(["condition_name", "icd10cm_code", "icd10cm_code_dotted", "code_role", "reference_period_years", "qualifying_claims_rule"])
    for c in conditions:
        name = re.sub(r"\s+", " ", " ".join(c["name"])).strip()
        name = re.sub(r"([-/]) ", r"\1", name)      # "Non- Alzheimer's" -> "Non-Alzheimer's"
        name = re.sub(r"\s\d$", "", name)            # drop footnote marker, e.g. "...Attack 1"
        rule = re.sub(r"\s+", " ", " ".join(c["qualify"])).strip()
        for code, role in c.get("codes", []):
            w.writerow([name, code.replace(".", ""), code, role, c["years"], rule])
print(len(conditions), "conditions")
for c in conditions:
    inc = sum(1 for _, r in c.get("codes", []) if r == "include")
    exc = sum(1 for _, r in c.get("codes", []) if r == "exclude")
    print(f'{" ".join(c["name"]):<60} yrs={c["years"]} include={inc:>5} exclude={exc:>4} | {re.sub(r"\s+"," "," ".join(c["qualify"]))[:70]}')