import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.math.BigDecimal

flowFile = session.get()
if (!flowFile) return

// ✅ Declare uniqueRecords outside so it can be used after session.write
def uniqueRecords = []

try {
    flowFile = session.write(flowFile, { inputStream, outputStream -> 
        def text = inputStream.getText(StandardCharsets.UTF_8.name())
        def json = new JsonSlurper().parseText(text)

        // --- Helper Transformation Functions ---

        def transformProductCode = { String product_code ->
            product_code?.toUpperCase() ?: "N/A"
        }

        def convertProductCategoryToStadardizedFormat = { String input ->
            if (!input?.trim()) return "N/A"
            def elements = input.split(/\s*\/\s*/)
                                .collect { it.trim().replaceAll("'", "''") }
            return elements.join(',')
        }

        def getLastProductCategory = { String val ->
            if (!val?.trim()) return "N/A"
            def parts = val.split('/')?.collect { it.trim() }?.findAll { it }
            return parts ? parts.last() : "N/A"
        }

        def transformProductType = { String product_type ->
            switch ((product_type ?: "").toLowerCase()) {
                case "consu":   return "Consumable"
                case "service": return "Service"
                case "product": return "Storable-Product"
                default:        return "N/A"
            }
        }

        def convertUnitCostToDecimal102 = { val, defaultVal = new BigDecimal("-1.00") ->
            try {
                if (!val?.toString()?.trim()) return defaultVal.setScale(2, BigDecimal.ROUND_HALF_UP)
                return new BigDecimal(val.toString().trim()).setScale(2, BigDecimal.ROUND_HALF_UP)
            } catch (Exception e) {
                return defaultVal.setScale(2, BigDecimal.ROUND_HALF_UP)
            }
        }

        def convertToTitleCase = { String val ->
            val ? val.toLowerCase().capitalize() : "N/A"
        }

        def transformIsComboItem = { Boolean val ->
            return (val == true) ? "Combo" : "Non-Combo"
        }

        // --- Process each item ---
        def records = (json instanceof List) ? json : [json]

        def seenIds = new HashSet()
        uniqueRecords = records.findAll { item ->
            if (seenIds.contains(item.id)) {
                return false
            } else {
                seenIds.add(item.id)
                return true
            }
        }

        uniqueRecords.each { item ->
            item.variant_id = item.id
            item.product_code = transformProductCode(item.product_code)
            item.name = item.name ?: "N/A"
            item.description = item.description ?: "N/A"

            def rawCategoryList = item.product_category_hierarchy

            item.product_category_hierarchy = convertProductCategoryToStadardizedFormat(rawCategoryList)
            item.product_category = getLastProductCategory(rawCategoryList)

            item.product_type = transformProductType(item.product_type)
            item.unit_price = convertUnitCostToDecimal102(item.unit_price)  
            item.unit_cost = convertUnitCostToDecimal102(item.unit_cost)
            item.product_brand = convertToTitleCase(item.product_brand)
            item.item_type = convertToTitleCase(item.item_type)
            item.product_classification_status = item.product_classification_status ?: "N/A"
            item.product_classification_1 = item.product_classification_1 ?: "N/A"
            item.product_classification_grade = item.product_classification_grade ?: "N/A"
            item.successor_model_product = item.successor_model_product ?: "N/A"
            item.is_combo_item = transformIsComboItem(item.is_combo_item)

            item.is_current = 1
            item.source_system_code = item.source_system_code ?: 1
        }

        def outputJson = JsonOutput.prettyPrint(JsonOutput.toJson(uniqueRecords))
        outputStream.write(outputJson.getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

    // ✅ Now this works, because uniqueRecords is defined outside the closure
    flowFile = session.putAttribute(flowFile, "record.count", uniqueRecords.size().toString())
    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error("Failed to process FlowFile due to: ${e.message}", e)
    if (flowFile != null) {
        session.transfer(flowFile, REL_FAILURE)
    }
}
