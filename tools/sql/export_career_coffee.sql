-- Export career.coffee postings as NDJSON (one JSON object per output row).
-- Schema generation A: the original platform. Int-coded enums (Category,
-- RoleType, EmploymentType, Status), parsed geo columns, skills stored as a
-- JSON array *serialised into a string* by EF Core — exported verbatim; the
-- Silver layer double-parses it. Runs via sqlcmd with :  -h -1 -y 0 -W
-- so each row prints as one raw NDJSON line.
--
-- Deliberately EXCLUDED (PII / noise policy — see tools/export_postings.py):
--   Description/Requirements/Responsibilities/Benefits  (bulk text, embeds
--     contact emails), ScrapedContactEmails, ScrapedContactPhones,
--     ApplicationEmail, ApplicationUrl (tracking links), PostedByUserId,
--     StreetAddress/PostalCode (over-precise), platform vanity fields
--     (ViewCount, ApplicationCount, IsFeatured, SeoFileId).
-- Soft-deleted rows are excluded; all other statuses are exported — expired
-- postings are valid history for weekly trend analysis.
-- NB: free-text fields are scrubbed of U+2028/U+2029/CR/LF. U+2028 is legal
-- UNESCAPED inside JSON strings, so FOR JSON passes it through raw and it
-- silently breaks line-oriented NDJSON readers (found the hard way: one
-- Japanese posting title). Newline-in-title carries no meaning; a space does.
SET NOCOUNT ON;

SELECT (SELECT
    'career.coffee'      AS board,
    'gen_a'              AS schema_gen,
    SYSUTCDATETIME()     AS exported_at,
    j.Id,
    REPLACE(REPLACE(REPLACE(REPLACE(j.Title, NCHAR(8232), ' '), NCHAR(8233), ' '), CHAR(13), ' '), CHAR(10), ' ') AS Title,
    j.Category, j.RoleType, j.EmploymentType,
    REPLACE(REPLACE(REPLACE(REPLACE(j.Location, NCHAR(8232), ' '), NCHAR(8233), ' '), CHAR(13), ' '), CHAR(10), ' ') AS Location,
    j.City, j.State, j.Country, j.IsRemote, j.IsHybrid,
    j.SalaryMin, j.SalaryMax, j.SalaryCurrency, j.SalaryPeriod,
    j.RequiredSkills, j.RequiredCertifications, j.MinYearsExperience,
    j.SourceUrl, j.Status,
    j.CreatedAt, j.UpdatedAt, j.LastSeenAt, j.ExpiresAt,
    j.Vertical,
    REPLACE(REPLACE(REPLACE(REPLACE(c.Name, NCHAR(8232), ' '), NCHAR(8233), ' '), CHAR(13), ' '), CHAR(10), ' ') AS CompanyName,
    c.Type AS CompanyType
  FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
FROM dbo.JobListings j
LEFT JOIN dbo.Companies c ON c.Id = j.CompanyId
WHERE j.DeletedAt IS NULL
  AND COALESCE(j.UpdatedAt, j.CreatedAt) >= '$(SINCE)'
ORDER BY j.Id;
