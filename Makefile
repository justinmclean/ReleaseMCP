PYTHON ?= python3

SKILL_NAME ?= incubator-releases
SKILL_SRC := skill/$(SKILL_NAME)
SKILL_BUNDLE := $(SKILL_NAME).skill

.PHONY: help test skill skill-check clean-skill

help:
	@echo "Available targets:"
	@echo "  test         Run unit tests"
	@echo "  skill        Build $(SKILL_BUNDLE) from $(SKILL_SRC)"
	@echo "  skill-check  Validate the skill source without building"
	@echo "  clean-skill  Remove the built skill bundle"

test:
	$(PYTHON) -m unittest discover -s tests -v

skill-check:
	@test -f $(SKILL_SRC)/SKILL.md || { echo "missing $(SKILL_SRC)/SKILL.md"; exit 1; }
	@head -1 $(SKILL_SRC)/SKILL.md | grep -qx -- '---' || { echo "$(SKILL_SRC)/SKILL.md must start with YAML frontmatter"; exit 1; }
	@grep -qx 'name: $(SKILL_NAME)' $(SKILL_SRC)/SKILL.md || { echo "$(SKILL_SRC)/SKILL.md name must be $(SKILL_NAME)"; exit 1; }
	@grep -q '^description:' $(SKILL_SRC)/SKILL.md || { echo "$(SKILL_SRC)/SKILL.md needs a description"; exit 1; }
	@echo "$(SKILL_SRC) looks well formed"

skill: skill-check clean-skill
	cd skill && zip -r -q -X ../$(SKILL_BUNDLE) $(SKILL_NAME) \
		-x '*.DS_Store' '*/__pycache__/*'
	@unzip -l $(SKILL_BUNDLE)

clean-skill:
	rm -f $(SKILL_BUNDLE)
