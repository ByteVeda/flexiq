//! The NATS sink's subject template.
//!
//! Parsed with the document, in every build, so a bad template is refused at
//! start even by a build without the NATS client.

use super::event::JobEvent;

/// A parsed subject such as `flexiq.events.{namespace}.{queue}.{type}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubjectTemplate {
    parts: Vec<Part>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    Literal(String),
    Field(Field),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Namespace,
    Queue,
    Task,
    Type,
}

impl Field {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "namespace" => Some(Self::Namespace),
            "queue" => Some(Self::Queue),
            "task" => Some(Self::Task),
            "type" => Some(Self::Type),
            _ => None,
        }
    }
}

impl SubjectTemplate {
    /// Parse a template, refusing anything that could render an invalid or
    /// wildcard subject.
    pub(crate) fn parse(template: &str) -> Result<Self, String> {
        let mut parts = Vec::new();
        let mut rest = template;
        while !rest.is_empty() {
            match rest.find(['{', '}']) {
                Some(at) if rest.as_bytes()[at] == b'{' => {
                    push_literal(&mut parts, &rest[..at])?;
                    let after = &rest[at + 1..];
                    let end = after
                        .find('}')
                        .ok_or_else(|| "subject has an unclosed '{'".to_string())?;
                    let name = &after[..end];
                    let field = Field::parse(name).ok_or_else(|| {
                        format!(
                            "subject names unknown field '{{{name}}}'; use {{namespace}}, \
                             {{queue}}, {{task}} or {{type}}"
                        )
                    })?;
                    parts.push(Part::Field(field));
                    rest = &after[end + 1..];
                }
                Some(_) => return Err("subject has a '}' with no '{'".into()),
                None => {
                    push_literal(&mut parts, rest)?;
                    rest = "";
                }
            }
        }
        let template = Self { parts };
        // Fields always render non-empty, so any empty token comes from the
        // literals: a leading, trailing or doubled '.'.
        let sample = template.render_with(|_| "x");
        if sample.is_empty() || sample.split('.').any(str::is_empty) {
            return Err("subject must not be empty, start or end with '.', or contain '..'".into());
        }
        Ok(template)
    }

    /// The subject for one event.
    #[cfg_attr(not(feature = "events-nats"), allow(dead_code))]
    pub(crate) fn render(&self, event: &JobEvent) -> String {
        self.render_with(|field| match field {
            Field::Namespace => event.namespace_label(),
            Field::Queue => &event.queue,
            Field::Task => &event.task_name,
            // A fixed set of dotted names such as `job.dead`, each a valid
            // pair of tokens, so a consumer can subscribe to `...job.>`.
            Field::Type => event.event_type.as_str(),
        })
    }

    fn render_with<'a>(&self, value: impl Fn(Field) -> &'a str) -> String {
        let mut subject = String::new();
        for part in &self.parts {
            match part {
                Part::Literal(text) => subject.push_str(text),
                Part::Field(Field::Type) => subject.push_str(value(Field::Type)),
                Part::Field(field) => push_token(&mut subject, value(*field)),
            }
        }
        subject
    }
}

fn push_literal(parts: &mut Vec<Part>, text: &str) -> Result<(), String> {
    if text.is_empty() {
        return Ok(());
    }
    if let Some(bad) = text.chars().find(|&c| !subject_char(c)) {
        return Err(format!("subject must not contain {bad:?}"));
    }
    parts.push(Part::Literal(text.to_string()));
    Ok(())
}

/// A queue, task or namespace name as one subject token: anything that would
/// split it (`.`), make it a wildcard (`*`, `>`) or break the protocol line
/// (whitespace, control characters) becomes `_`, and an empty name is `_`.
fn push_token(subject: &mut String, value: &str) {
    if value.is_empty() {
        subject.push('_');
        return;
    }
    subject.extend(
        value
            .chars()
            .map(|c| if c != '.' && subject_char(c) { c } else { '_' }),
    );
}

fn subject_char(c: char) -> bool {
    !(c == '*' || c == '>' || c.is_whitespace() || c.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::event::EventType;

    fn event(namespace: Option<&str>, queue: &str, task: &str) -> JobEvent {
        JobEvent::new(
            EventType::JobDead,
            "j",
            namespace.map(str::to_string),
            queue,
            task,
        )
    }

    #[test]
    fn fields_render_and_type_keeps_its_dot() {
        let template = SubjectTemplate::parse("flexiq.{namespace}.{queue}.{task}.{type}").unwrap();
        assert_eq!(
            template.render(&event(None, "emails", "send")),
            "flexiq.default.emails.send.job.dead"
        );
        let fixed = SubjectTemplate::parse("flexiq.events").unwrap();
        assert_eq!(fixed.render(&event(None, "q", "t")), "flexiq.events");
    }

    #[test]
    fn a_name_cannot_add_tokens_or_wildcards() {
        let template = SubjectTemplate::parse("e.{namespace}.{queue}.{task}").unwrap();
        assert_eq!(
            template.render(&event(Some("a.b"), "q *>", "")),
            "e.a_b.q___._"
        );
    }

    #[test]
    fn a_field_may_share_a_token_with_a_literal() {
        let template = SubjectTemplate::parse("e.q-{queue}").unwrap();
        assert_eq!(template.render(&event(None, "x", "t")), "e.q-x");
    }

    #[test]
    fn bad_templates_are_refused() {
        for (template, needle) in [
            ("", "empty"),
            (".e", "'..'"),
            ("e.", "'..'"),
            ("e..f", "'..'"),
            ("e.*", "'*'"),
            ("e.>", "'>'"),
            ("e f", "' '"),
            ("e.{queue", "unclosed"),
            ("e.queue}", "no '{'"),
            ("e.{job}", "unknown field '{job}'"),
            ("e.{}", "unknown field '{}'"),
        ] {
            let error = SubjectTemplate::parse(template).unwrap_err();
            assert!(error.contains(needle), "{template:?}: {error}");
        }
    }
}
