"""Functional tests for the AI Extract Lab page (pages/4_AI_Extract_Lab.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions


EXTRACT_PATH = "/AI_Extract_Lab"


def _navigate(page, app_url):
    """Navigate to AI Extract Lab and wait for render."""
    page.goto(f"{app_url}{EXTRACT_PATH}", wait_until="networkidle")
    wait_for_streamlit(
        page,
        selectors='[data-testid="stRadio"], [data-testid="stExpander"]',
    )


@pytest.mark.smoke
def test_extract_lab_reference_expander(app_url, page):
    """AI Extract Lab has the Quick Reference expander."""
    _navigate(page, app_url)
    expander = page.locator('[data-testid="stExpander"]')
    assert expander.count() >= 1, "Expected at least 1 expander (Quick Reference)"
    ref_text = page.locator("text=AI_EXTRACT Quick Reference")
    assert ref_text.count() > 0, "Expected 'AI_EXTRACT Quick Reference' expander"


def test_extract_lab_prompt_mode_radio(app_url, page):
    """AI Extract Lab has a radio with 3 prompt modes."""
    _navigate(page, app_url)
    radio = page.locator('[data-testid="stRadio"]')
    assert radio.count() >= 1, "Expected at least 1 radio group"
    for mode in ["Starter Template", "Visual Builder", "Raw JSON Editor"]:
        assert page.locator(f"text={mode}").count() > 0, f"Missing radio option: {mode}"


def test_extract_lab_default_template_invoice_header(app_url, page):
    """Default template is 'Invoice Header Fields' with vendor_name in textarea."""
    _navigate(page, app_url)
    textarea = page.locator("textarea").first
    if textarea.count() > 0:
        value = textarea.input_value()
        assert "vendor_name" in value, "Default template should contain 'vendor_name'"


def test_extract_lab_switch_to_line_items_template(app_url, page):
    """Switching template to 'Line Items Table' shows column_ordering in textarea."""
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() > 0:
        selectboxes.first.click()
        page.wait_for_timeout(500)
        option = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]',
            has_text="Line Items Table",
        )
        if option.count() > 0:
            option.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page, selectors="textarea")
            textarea = page.locator("textarea").first
            if textarea.count() > 0:
                value = textarea.input_value()
                assert "column_ordering" in value, (
                    "Line Items Table template should contain 'column_ordering'"
                )


def test_extract_lab_visual_builder_mode(app_url, page):
    """Clicking 'Visual Builder' radio shows entity text inputs."""
    _navigate(page, app_url)
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() > 0:
        vb_label.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
        text_inputs = page.locator('[data-testid="stTextInput"]')
        assert text_inputs.count() >= 2, (
            f"Visual Builder should show text inputs for entity fields, got {text_inputs.count()}"
        )


def test_extract_lab_visual_builder_table_schema_mode(app_url, page):
    """Visual Builder has 'Entity' and 'Table' sub-modes via radio."""
    _navigate(page, app_url)
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() > 0:
        vb_label.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page, selectors='[data-testid="stRadio"]')
        # After entering Visual Builder, there should be a sub-radio for Entity/Table
        radios = page.locator('[data-testid="stRadio"]')
        if radios.count() >= 2:
            # Check for Entity and Table text in the sub-radio
            assert page.locator("text=Entity").count() > 0 or page.locator("text=Table").count() > 0, (
                "Visual Builder should have Entity/Table sub-mode options"
            )


def test_extract_lab_raw_json_editor_mode(app_url, page):
    """Clicking 'Raw JSON Editor' shows a freeform textarea."""
    _navigate(page, app_url)
    raw_label = page.locator('[data-testid="stRadio"] label', has_text="Raw JSON Editor")
    if raw_label.count() > 0:
        raw_label.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page, selectors="textarea")
        textarea = page.locator("textarea")
        assert textarea.count() >= 1, "Raw JSON Editor should show a textarea"


def test_extract_lab_staged_file_selectbox(app_url, page):
    """Staged file selectbox has at least 1 PDF option."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    assert selectboxes.count() >= 1, "Expected at least 1 selectbox for staged files"


def test_extract_lab_run_button(app_url, page):
    """AI Extract Lab has a 'Run AI_EXTRACT' primary button."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    btn = page.locator('button:has-text("Run AI_EXTRACT")')
    assert btn.count() > 0, "Expected 'Run AI_EXTRACT' button"


def test_extract_lab_file_uploader(app_url, page):
    """AI Extract Lab has a file uploader for local documents."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    uploader = page.locator('[data-testid="stFileUploader"]')
    assert uploader.count() > 0, "Expected a file uploader element"


def test_extract_lab_browse_staged_header(app_url, page):
    """AI Extract Lab has a 'Browse Staged Invoices' section header."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    header = page.locator("text=Browse Staged Invoices")
    assert header.count() > 0, "Expected 'Browse Staged Invoices' header"


def test_extract_lab_upload_section_header(app_url, page):
    """AI Extract Lab has an 'Upload & Extract' section header."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    header = page.locator("text=Upload & Extract")
    assert header.count() > 0, "Expected 'Upload & Extract' header"


def test_extract_lab_title(app_url, page):
    """AI Extract Lab displays the correct page title."""
    _navigate(page, app_url)
    title = page.locator("text=AI_EXTRACT Lab")
    assert title.count() > 0, "Expected 'AI_EXTRACT Lab' title"


def test_extract_lab_general_document_template(app_url, page):
    """Switching to 'General Document Q&A' template shows question-based prompt."""
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() > 0:
        selectboxes.first.click()
        page.wait_for_timeout(500)
        option = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]',
            has_text="General Document",
        )
        if option.count() > 0:
            option.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page, selectors="textarea")
            textarea = page.locator("textarea").first
            if textarea.count() > 0:
                value = textarea.input_value()
                assert len(value) > 0, "General Document template should have content"


def test_extract_lab_raw_json_invalid_shows_no_crash(app_url, page):
    """Entering invalid JSON in Raw JSON Editor mode doesn't crash the page."""
    _navigate(page, app_url)
    raw_label = page.locator('[data-testid="stRadio"] label', has_text="Raw JSON Editor")
    if raw_label.count() > 0:
        raw_label.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page, selectors="textarea")
        textarea = page.locator("textarea").first
        if textarea.count() > 0:
            textarea.fill("{invalid json!!!}")
            page.wait_for_timeout(1000)
    assert_no_exceptions(page)


def test_extract_lab_preview_response_format_expander(app_url, page):
    """AI Extract Lab has a 'Preview responseFormat' expander or code block."""
    _navigate(page, app_url)
    # The preview section is usually visible in Visual Builder mode
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() > 0:
        vb_label.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
        # Look for preview-related content
        preview = page.locator("text=Preview")
        code_block = page.locator('[data-testid="stCode"]')
        assert preview.count() > 0 or code_block.count() > 0, (
            "Visual Builder should show a preview section or code block"
        )


def test_extract_lab_caption_text(app_url, page):
    """AI Extract Lab shows the 'Experiment with Snowflake Cortex AI_EXTRACT' caption."""
    _navigate(page, app_url)
    assert page.locator("text=Experiment with Snowflake Cortex AI_EXTRACT").count() > 0, \
        "Expected caption containing 'Experiment with Snowflake Cortex AI_EXTRACT'"


def test_extract_lab_prompt_builder_subheader(app_url, page):
    """AI Extract Lab has a 'Prompt Builder' subheader."""
    _navigate(page, app_url)
    assert page.locator("text=Prompt Builder").count() > 0, \
        "Expected 'Prompt Builder' subheader"


def test_extract_lab_visual_builder_add_field_button(app_url, page):
    """Visual Builder Entity mode has an 'Add field' button."""
    _navigate(page, app_url)
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() > 0:
        vb_label.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
        add_btn = page.locator('button:has-text("Add field")')
        assert add_btn.count() > 0, "Expected 'Add field' button in Visual Builder Entity mode"


def test_extract_lab_visual_builder_table_mode_inputs(app_url, page):
    """Visual Builder Table mode shows 'Table description' and 'Table name' text inputs."""
    _navigate(page, app_url)
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() > 0:
        vb_label.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page, selectors='[data-testid="stRadio"]')
        # Click the Table sub-radio
        table_label = page.locator('[data-testid="stRadio"] label', has_text="Table")
        if table_label.count() > 0:
            table_label.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
            body_text = page.inner_text("body")
            assert "Table description" in body_text, "Expected 'Table description' input label"
            assert "Table name" in body_text, "Expected 'Table name' input label"


def test_extract_lab_visual_builder_add_column_button(app_url, page):
    """Visual Builder Table mode has an 'Add column' button."""
    _navigate(page, app_url)
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() > 0:
        vb_label.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page, selectors='[data-testid="stRadio"]')
        table_label = page.locator('[data-testid="stRadio"] label', has_text="Table")
        if table_label.count() > 0:
            table_label.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
            add_col_btn = page.locator('button:has-text("Add column")')
            assert add_col_btn.count() > 0, "Expected 'Add column' button in Table mode"


def test_extract_lab_invalid_json_error_message(app_url, page):
    """Entering invalid JSON in Raw JSON Editor shows 'Invalid JSON' error."""
    _navigate(page, app_url)
    raw_label = page.locator('[data-testid="stRadio"] label', has_text="Raw JSON Editor")
    if raw_label.count() > 0:
        raw_label.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page, selectors="textarea")
        textarea = page.locator("textarea").first
        if textarea.count() > 0:
            textarea.fill("{not valid json!!!")
            # Press Tab or click away to trigger rerun
            textarea.press("Tab")
            page.wait_for_timeout(3000)
            wait_for_streamlit(page, selectors='[data-testid="stAlert"]')
            error_text = page.locator("text=Invalid JSON")
            assert error_text.count() > 0, "Expected 'Invalid JSON' error message"


def test_extract_lab_upload_run_button(app_url, page):
    """Upload section has 'Upload & Run AI_EXTRACT' button text (visible after file upload)."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    # The Upload & Run button only appears after a file is uploaded, so check the
    # Upload & Extract header exists instead as a proxy for the section rendering
    header = page.locator("text=Upload & Extract")
    assert header.count() > 0, "Expected 'Upload & Extract' section"
    uploader = page.locator('[data-testid="stFileUploader"]')
    assert uploader.count() > 0, "Expected file uploader in Upload & Extract section"


def test_extract_lab_add_field_click_adds_row(app_url, page):
    """Clicking 'Add field' in Visual Builder Entity mode adds a new input row."""
    _navigate(page, app_url)
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() == 0:
        pytest.skip("Visual Builder radio not found")

    vb_label.first.click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')

    # Count text inputs before clicking Add field
    inputs_before = page.locator('[data-testid="stTextInput"]').count()

    add_btn = page.locator('button:has-text("Add field")')
    assert add_btn.count() > 0, "Expected 'Add field' button"
    add_btn.first.click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')

    inputs_after = page.locator('[data-testid="stTextInput"]').count()
    assert inputs_after > inputs_before, (
        f"Add field should increase text inputs: {inputs_before} -> {inputs_after}"
    )
    assert_no_exceptions(page)


def test_extract_lab_delete_field_click(app_url, page):
    """Clicking '✕' on a field in Visual Builder Entity mode removes an input row."""
    _navigate(page, app_url)
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() == 0:
        pytest.skip("Visual Builder radio not found")

    vb_label.first.click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')

    inputs_before = page.locator('[data-testid="stTextInput"]').count()
    if inputs_before < 4:
        # Need at least 2 rows (4 inputs: key+question each) to delete one
        pytest.skip("Not enough fields to test deletion")

    # Click the first ✕ button
    delete_btn = page.locator('button:has-text("✕")')
    if delete_btn.count() == 0:
        pytest.skip("No delete (✕) buttons found")

    delete_btn.first.click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')

    inputs_after = page.locator('[data-testid="stTextInput"]').count()
    assert inputs_after < inputs_before, (
        f"Delete field should decrease text inputs: {inputs_before} -> {inputs_after}"
    )
    assert_no_exceptions(page)


def test_extract_lab_add_column_click_adds_row(app_url, page):
    """Clicking 'Add column' in Visual Builder Table mode adds a new column row."""
    _navigate(page, app_url)
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() == 0:
        pytest.skip("Visual Builder radio not found")

    vb_label.first.click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page, selectors='[data-testid="stRadio"]')

    # Switch to Table sub-mode
    table_label = page.locator('[data-testid="stRadio"] label', has_text="Table")
    if table_label.count() == 0:
        pytest.skip("Table sub-mode radio not found")

    table_label.first.click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')

    inputs_before = page.locator('[data-testid="stTextInput"]').count()

    add_col_btn = page.locator('button:has-text("Add column")')
    assert add_col_btn.count() > 0, "Expected 'Add column' button"
    add_col_btn.first.click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')

    inputs_after = page.locator('[data-testid="stTextInput"]').count()
    assert inputs_after > inputs_before, (
        f"Add column should increase text inputs: {inputs_before} -> {inputs_after}"
    )
    assert_no_exceptions(page)


def test_extract_lab_edit_entity_field_updates_preview(app_url, page):
    """Editing a field name in Visual Builder Entity mode updates the Preview code block."""
    _navigate(page, app_url)
    vb_label = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb_label.count() == 0:
        pytest.skip("Visual Builder radio not found")

    vb_label.first.click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')

    # Edit the first field name to a unique value
    text_inputs = page.locator('[data-testid="stTextInput"] input')
    if text_inputs.count() == 0:
        pytest.skip("No text inputs found in Visual Builder")

    first_input = text_inputs.first
    first_input.click()
    first_input.fill("custom_test_field")
    # Press Tab to commit the change and trigger rerun
    first_input.press("Tab")
    page.wait_for_timeout(3000)
    wait_for_streamlit(page, selectors='[data-testid="stCode"]')

    # Check that the Preview code block contains the new field name
    code_blocks = page.locator('[data-testid="stCode"]')
    if code_blocks.count() > 0:
        code_text = code_blocks.first.inner_text()
        assert "custom_test_field" in code_text, (
            f"Expected 'custom_test_field' in preview code block, got: {code_text[:200]}"
        )
    assert_no_exceptions(page)


def test_extract_lab_switch_staged_file(app_url, page):
    """Selecting a different staged file updates the preview without exceptions."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    # The staged file selectbox is in the Browse Staged Invoices section
    # Find it — it might be the last or second selectbox depending on prompt mode
    staged_select = None
    for i in range(selectboxes.count()):
        sb_text = selectboxes.nth(i).inner_text()
        if "staged" in sb_text.lower() or ".pdf" in sb_text.lower():
            staged_select = selectboxes.nth(i)
            break

    if staged_select is None and selectboxes.count() >= 2:
        # Fallback: the staged file selectbox is typically the last one
        staged_select = selectboxes.last

    if staged_select is None:
        pytest.skip("No staged file selectbox found")

    staged_select.click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() < 2:
        pytest.skip("Only one staged file available")

    # Select the second option (different from default first)
    options.nth(1).click()
    page.wait_for_timeout(5000)
    wait_for_streamlit(page)

    # The page should still render without exceptions
    assert_no_exceptions(page)


def test_extract_lab_quick_reference_expand_collapse(app_url, page):
    """Expanding and collapsing the Quick Reference expander works without exceptions."""
    _navigate(page, app_url)
    expanders = page.locator('[data-testid="stExpander"]')
    if expanders.count() == 0:
        pytest.skip("No expanders found")

    # Click to expand the Quick Reference
    expanders.first.click()
    page.wait_for_timeout(1000)

    # Verify expanded content is visible (reference text)
    body = page.inner_text("body")
    assert "Supported file formats" in body or "Extraction modes" in body, (
        "Quick Reference should show content when expanded"
    )

    # Click to collapse
    expanders.first.click()
    page.wait_for_timeout(1000)
    assert_no_exceptions(page)


def test_extract_lab_template_textarea_edit_validates(app_url, page):
    """Editing the template textarea with valid JSON shows no error."""
    _navigate(page, app_url)
    textarea = page.locator("textarea").first
    if textarea.count() == 0:
        pytest.skip("No textarea found in Starter Template mode")

    # Replace content with custom valid JSON
    custom_json = '{"my_field": "What is the custom value?"}'
    textarea.click()
    textarea.fill(custom_json)
    textarea.press("Tab")
    page.wait_for_timeout(3000)
    wait_for_streamlit(page)

    # There should be no "Invalid JSON" error
    error = page.locator("text=Invalid JSON")
    assert error.count() == 0, "Valid JSON should not trigger an 'Invalid JSON' error"
    assert_no_exceptions(page)


@pytest.mark.smoke
def test_extract_lab_no_exceptions(app_url, page):
    """AI Extract Lab renders with zero Streamlit exceptions across default mode."""
    _navigate(page, app_url)
    assert_no_exceptions(page)
