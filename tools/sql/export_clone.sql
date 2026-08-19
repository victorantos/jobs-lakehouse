-- Export postings from a "generation C" board (solar / pet / delivery /
-- church / dental — clean clones of one codebase) as NDJSON.
-- Parameterised: run with  sqlcmd -d <BoardDb> -v BOARD="career.xxx" SINCE="..."
-- Gen C has NO parsed geo columns (only the free-text Location string) and no
-- Category — that absence is real and is precisely why Silver parses
-- locations itself. Tags folded to a JSON array (often null; solar is rich).
-- Same exclusion policy as the coffee export (see that file / the runner).
SET NOCOUNT ON;

SELECT (SELECT
    '$(BOARD)'           AS board,
    'gen_c'              AS schema_gen,
    SYSUTCDATETIME()     AS exported_at,
    j.Id,
    REPLACE(REPLACE(REPLACE(REPLACE(j.Title, NCHAR(8232), ' '), NCHAR(8233), ' '), CHAR(13), ' '), CHAR(10), ' ') AS Title,
    j.JobType, j.ExperienceLevel,
    REPLACE(REPLACE(REPLACE(REPLACE(j.Location, NCHAR(8232), ' '), NCHAR(8233), ' '), CHAR(13), ' '), CHAR(10), ' ') AS Location,
    j.IsRemote,
    j.SalaryMin, j.SalaryMax, j.SalaryCurrency, j.SalaryPeriod,
    j.SourceUrl, j.SourceHash, j.Status,
    j.PostedAt, j.CreatedAt, j.ExpiresAt,
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
WHERE j.CreatedAt >= '$(SINCE)'
ORDER BY j.Id;
