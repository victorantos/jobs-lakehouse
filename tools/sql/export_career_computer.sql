-- Export career.computer postings as NDJSON (one JSON object per output row).
-- Schema generation B: newer platform, string-coded enums (JobType holds 120+
-- raw scraped values incl. CDI, Ausbildung, 正社員), parsed geo columns,
-- normalised tags (JobTags -> Tags) folded back into a JSON array per row so
-- every board exports the same *shape* of file: postings with embedded tags.
-- Same exclusion policy as the coffee export (see that file / the runner).
SET NOCOUNT ON;

SELECT (SELECT
    'career.computer'    AS board,
    'gen_b'              AS schema_gen,
    SYSUTCDATETIME()     AS exported_at,
    j.Id,
    REPLACE(REPLACE(REPLACE(REPLACE(j.Title, NCHAR(8232), ' '), NCHAR(8233), ' '), CHAR(13), ' '), CHAR(10), ' ') AS Title,
    j.JobType, j.ExperienceLevel, j.Category,
    REPLACE(REPLACE(REPLACE(REPLACE(j.Location, NCHAR(8232), ' '), NCHAR(8233), ' '), CHAR(13), ' '), CHAR(10), ' ') AS Location,
    j.City, j.State, j.Country, j.IsRemote, j.RemoteType,
    j.SalaryMin, j.SalaryMax, j.SalaryCurrency, j.SalaryPeriod,
    j.SourceUrl, j.SourceHash, j.Status,
    j.PostedAt, j.CreatedAt, j.UpdatedAt, j.ExpiresAt,
    REPLACE(REPLACE(REPLACE(REPLACE(c.Name, NCHAR(8232), ' '), NCHAR(8233), ' '), CHAR(13), ' '), CHAR(10), ' ') AS CompanyName,
    JSON_QUERY(tags.arr) AS Tags
  FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
FROM dbo.Jobs j
LEFT JOIN dbo.Companies c ON c.Id = j.CompanyId
OUTER APPLY (
    SELECT '[' + STRING_AGG('"' + STRING_ESCAPE(t.Name, 'json') + '"', ',') + ']' AS arr
    FROM dbo.JobTags jt
    JOIN dbo.Tags t ON t.Id = jt.TagId
    WHERE jt.JobId = j.Id
) tags
WHERE COALESCE(j.UpdatedAt, j.CreatedAt) >= '$(SINCE)'
ORDER BY j.Id;
