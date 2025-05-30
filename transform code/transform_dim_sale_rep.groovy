import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->

    def text = inputStream.getText(StandardCharsets.UTF_8.name())
    def json = new JsonSlurper().parseText(text)

    // --- Helper Transformation Functions ---

    def transformCode = { String code ->
        if (code == null) {
            return "N/A"
        } else {
            return code.toUpperCase()
        }
    }

    def transformPhone = { String phone ->
        if (!phone) return "N/A"
        return phone.startsWith("0") ? "855" + phone.substring(1) : phone
    }

    def transformStaffType = { String staffType ->
        switch ((staffType ?: "").toLowerCase()) {
            case "support":
                return "Support-Staff"
            case "field":
                return "Field-Staff"
            case "other":
                return "Other"
            default:
                return "N/A"
        }
    }

    def transformBranch = { String branch ->
        if (branch == null) {
            return "N/A"
        } else {
            return branch.toUpperCase()
        }
    }

    // --- Transform start_date to surrogate date_key directly ---
    def formatDateFieldToDateKey = { Object startDateRaw ->
        def zoneId = ZoneId.of("Asia/Bangkok")  // UTC+7
        try {
            if (startDateRaw == null) return 99991231

            def rawStr = startDateRaw.toString()
            if (!rawStr.isLong()) return 99991231

            def instant = Instant.ofEpochMilli(rawStr.toLong())
            def date = instant.atZone(zoneId).toLocalDate()
            return date.format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
        } catch (Exception e) {
            return 99991231
        }
    }

    // --- Process each item ---
    json.each { item ->
        item.name = item.name ?: "N/A"
        item.code = transformCode(item.code)
        item.firstname = item.firstname ?: "N/A"
        item.lastname = item.lastname ?: "N/A"
        item.fullname = item.fullname ?: "N/A"
        item.staff_type = transformStaffType(item.staff_type)
        item.phone = transformPhone(item.phone)
        item.email = item.email ?: "N/A"
        item.branch = transformBranch(item.branch)

        // Overwrite start_date with date_key (FK to dim_date)
        item.start_date = formatDateFieldToDateKey(item.start_date)

        item.department = item.department ?: "N/A"
        item.job_position = item.job_position ?: "N/A"
        item.job_grade = item.job_grade ?: "N/A"
        item.active = item.active

        item.is_current = 1
        item.source_system_code = item.source_system_code ?: 1
    }

    def outputJson = JsonOutput.prettyPrint(JsonOutput.toJson(json))
    outputStream.write(outputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
