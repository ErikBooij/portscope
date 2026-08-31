# Security policy

Portscope processes credentials and plaintext application traffic by design. Do not report suspected vulnerabilities in a public issue.

Use [GitHub private vulnerability reporting](https://github.com/erikbooij/portscope/security/advisories/new) and include:

- the affected version or commit;
- a minimal reproduction;
- the security impact; and
- any suggested mitigation.

You should receive an acknowledgement within seven days. Please allow time for a fix and release before public disclosure.

Only the latest released version is supported with security fixes. Portscope is a local development tool: keep the management listener on loopback or place it behind an authenticated ingress, and never expose proxy listeners beyond the network scope your application requires.
