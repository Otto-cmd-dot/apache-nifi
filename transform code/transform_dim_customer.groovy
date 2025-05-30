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
    def transformTitleCase = { String firstname ->
        if (firstname == null) return "N/A"
        return firstname.toLowerCase().capitalize()
    }

    def transformPhone = { String phone ->
        if (!phone) return "N/A"
        return phone.startsWith("0") ? "855" + phone.substring(1) : phone
    }


    def convertToDecimal96 = { val ->
        try {
            return (val != null && val.toString().isNumber()) ? new BigDecimal(val).setScale(2, BigDecimal.ROUND_HALF_UP) : new BigDecimal("0.00")
        } catch (e) {
            return new BigDecimal("0.00")
        }
    }


    def transformGender = { String gender ->
        if (gender == null) return "N/A"
        return gender.toLowerCase().capitalize()
    }


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
    def records = (json instanceof List) ? json : [json]
        records.each { item ->
            item.name = item.name?.trim() ?: "N/A"
            item.firstname = transformTitleCase(item.firstname)
            item.middlename = item.middlename ?: "N/A"
            item.lastname = transformTitleCase(item.lastname)
            item.title = transformTitleCase(item.title)
            item.address = item.address ?: "N/A"
            item.country = item.country ?: "N/A"
            item.email = item.email ?: "N/A"
            item.phone = transformPhone(item.phone)
            item.province = item.province ?: "N/A"
            item.district = item.district ?: "N/A"
            item.commune = item.commune ?: "N/A"
            item.village = item.village ?: "N/A"
            item.postcode = item.postcode ?: "N/A"

            item.latitude = convertToDecimal96(item.latitude)
            item.longitude = convertToDecimal96(item.longitude)

            item.marital_status = transformTitleCase(item.marital_status)
            item.dob = formatDateFieldToDateKey(item.dob)
            item.gender = transformGender(item.gender)
            item.province_code = item.province_code ?: "N/A"
            item.is_current = 1
            item.source_system_code = item.source_system_code
    }

    def outputJson = JsonOutput.prettyPrint(JsonOutput.toJson(json))
    outputStream.write(outputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
