SELECT p.id, created_on, title, description, tag
FROM `made-with-ml-384201.mlops_course.projects` p
LEFT JOIN `made-with-ml-384201.mlops_course.tags` t
ON p.id = t.id
