//
// Created by deamon on 02.04.18.
//

#include <stdint.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <assert.h>
#include <cstring>
#include <cmath>
#include <iomanip>
#include "DB2Ver3.h"

using namespace WDC3;

static const inline bool checkDataIfNonZero(unsigned char *ptr, int length) {
    for (int i = 0; i < length; i++) {
        if (ptr[i] != 0)
            return false;
    }

    return true;
}

void DB2Ver3::process(HFileContent db2File, const std::string &fileName) {
    this->db2File = db2File;
    this->db2FileName = fileName;
    fileData = &(*this->db2File.get())[0];

    currentOffset = 0;
    bytesRead = 0;

    readValue(header);

    readValues(section_headers, header->section_count);
    readValues(fields, header->total_field_count);
    fieldInfoLength = header->field_storage_info_size / sizeof(field_storage_info);
    readValues(field_info, fieldInfoLength);


    int palleteDataRead = 0;
    if (header->pallet_data_size > 0) {
        palleteDataArray.resize(header->field_count);
        for (int i = 0; i < header->field_count; i++) {
            if ((field_info[i].storage_type == field_compression::field_compression_bitpacked_indexed) ||
                (field_info[i].storage_type == field_compression::field_compression_bitpacked_indexed_array)) {

                for (int j = 0; j < field_info[i].additional_data_size / 4; j++) {
                    uint32_t value;
                    readValue(value);
                    palleteDataArray[i].push_back(value);
                    palleteDataRead++;
                }
            }
        }
    }

    assert(palleteDataRead*4 == header->pallet_data_size);

//    readValues(pallet_data, header->pallet_data_size);
    //Form hashtable for column

    if (header->common_data_size > 0) {
        commonDataHashMap.resize(header->field_count);
        for (int i = 0; i < header->field_count; i++) {
            if (field_info[i].storage_type == field_compression::field_compression_common_data) {
                int id;
                uint32_t value;
                for (int j = 0; j < field_info[i].additional_data_size / 8; j++) {
                    readValue(id);
                    readValue(value);

                    commonDataHashMap[i][id] = value;
                }
            }
        }
    }
//    readValues(common_data, );

    //Read section
    sections.resize(header->section_count);

    for (int i = 0; i < header->section_count; i++) {
        auto &itemSectionHeader = section_headers[i];

        section &section = sections[i];

//        if (itemSectionHeader.tact_key_hash != 0) break;

        assert(itemSectionHeader.file_offset == currentOffset);

        if (!header->flags.isSparse) {
            // Normal records

            for (int j = 0; j < itemSectionHeader.record_count; j++) {
                record_data recordData;
                readValues(recordData.data, header->record_size);

                section.records.push_back(recordData);
            }
            readValues(section.string_data, itemSectionHeader.string_table_size);
        } else {
            // Offset map records -- these records have null-terminated strings inlined, and
            // since they are variable-length, they are pointed to by an array of 6-byte
            // offset+size pairs.
            readValues(section.variable_record_data, itemSectionHeader.offset_records_end - itemSectionHeader.file_offset);
        }

        if (itemSectionHeader.offset_records_end > 0) {
            assert(itemSectionHeader.offset_records_end == currentOffset);
        }

        readValues(section.id_list, itemSectionHeader.id_list_size / 4);

        if (itemSectionHeader.copy_table_count > 0) {
            readValues(section.copy_table, itemSectionHeader.copy_table_count);
        }

        if (header->table_hash == 145293629)
            currentOffset+=itemSectionHeader.offset_map_id_count*4;

        readValues(section.offset_map, itemSectionHeader.offset_map_id_count);

        auto offsetBeforRelationshipData = currentOffset;
        if (itemSectionHeader.relationship_data_size > 0) {
            // In some tables, this relationship mapping replaced columns that were used
            // only as a lookup, such as the SpellID in SpellX* tables.
            readValue(section.relationship_map.num_entries);
            readValue(section.relationship_map.min_id);
            readValue(section.relationship_map.max_id);
            readValues(section.relationship_map.entries, section.relationship_map.num_entries);

            hasRelationShipField |= (section.relationship_map.num_entries > 0);

            for (int relationInd = 0; relationInd < section.relationship_map.num_entries; relationInd++) {
                const auto &entry = section.relationship_map.entries[relationInd];
                section.perRecordIndexRelation[entry.record_index] = entry.foreign_id;
            }

        }
        if (offsetBeforRelationshipData + itemSectionHeader.relationship_data_size != currentOffset) {
            currentOffset = offsetBeforRelationshipData + itemSectionHeader.relationship_data_size;
        }

        readValues(section.offset_map_id_list, itemSectionHeader.offset_map_id_count);

        if (itemSectionHeader.tact_key_hash != 0)
        {
            //Check if section was decrypted properly
            if (!header->flags.isSparse) {
                section.isEncoded = checkDataIfNonZero(section.records[0].data, currentOffset - itemSectionHeader.file_offset);
            }
            else {
                section.isEncoded = checkDataIfNonZero(section.variable_record_data, currentOffset - itemSectionHeader.file_offset);
            }
        }
    }

    m_loaded = true;
}


bool get_bit(unsigned char *data, unsigned bitoffset) // returns the n-th bit
{
    int c = (int)(data[bitoffset >> 3]); // X>>3 is X/8
    int bitmask = 1 << (bitoffset & 7);  // X&7 is X%8
    return ((c & bitmask)!=0);
}

uint32_t get_bits(unsigned char* data, unsigned bitOffset, unsigned numBits)
{
    unsigned int bits = 0;
    int bitPos = 0;
    for (int currentbit = bitOffset; currentbit < bitOffset + numBits; currentbit++)
    {
        bits = bits | (get_bit(data, currentbit) << bitPos++);
    }
    return bits;
}


void extractBits(unsigned char *inputBuffer, unsigned char *outputBuffer, int bitOffset, int bitLength) {
    unsigned int byteOffset = (bitOffset) >> 3;
    bitOffset = bitOffset & 7;
    //Read bites

    unsigned char headMask = 0xFFu << (bitOffset);
    unsigned char tailMask = (unsigned char) (0xFFu ^ headMask);

    int totalBytesToRead = ((bitOffset+bitLength) + 7) >> 3;
    if (totalBytesToRead > 1) {
        uint8_t headByte = 0; //recordPointer[byteOffset] & (headMask);
        uint8_t tailByte = 0;

        if (bitLength > 8) {
            for (int j = 0; j < (bitLength >> 3); j++) {
                headByte = inputBuffer[byteOffset + j] & headMask;
                tailByte = inputBuffer[byteOffset + j + 1] & (tailMask);

                outputBuffer[j] = (headByte) >> bitOffset;
                outputBuffer[j] = outputBuffer[j] | ((tailByte) << (8 - bitOffset));
            }
        } else {
            headByte = inputBuffer[byteOffset] & headMask;
            outputBuffer[0] = (headByte) >> bitOffset;
        }

        //TODO: think through this part a little better
        if (((bitOffset + bitLength) & 7) > 0) {
            tailMask = (0xFFu >> ( 8 - ((bitOffset + bitLength) & 7)));
        } else {
            tailMask = 0xffu;
        }
        if (bitLength > 8) {
            headMask = headMask & tailMask;
            headByte = inputBuffer[byteOffset + totalBytesToRead-1] & (headMask);
            outputBuffer[totalBytesToRead-1] = outputBuffer[totalBytesToRead-1] | ((headByte) >> bitOffset);
        } else {
            tailByte = inputBuffer[byteOffset + totalBytesToRead-1] & (tailMask);
            outputBuffer[0] = outputBuffer[0] | ((tailByte) << (8 - bitOffset));
        }
//                    int endByteOffset = totalBytesToRead - 1;

    } else {
        tailMask = (0xFFu >> (8 - ((bitOffset + bitLength) & 7)));
        headMask = headMask & tailMask;

        uint8_t headByte = inputBuffer[byteOffset] & (headMask);
        outputBuffer[0] = (headByte) >> bitOffset;
    }
}

bool DB2Ver3::getSectionIndex(int recordIndex, int &sectionIndex, int &indexWithinSection) const {
    //Find Record by section
    sectionIndex = 0;
    indexWithinSection = recordIndex;

    while (indexWithinSection >= section_headers[sectionIndex].record_count) {
        indexWithinSection -= section_headers[sectionIndex].record_count;
        sectionIndex++;
        if (sectionIndex >= sections.size())
            return false;
    }

    auto &sectionContent = sections[sectionIndex];
    //
    if (sectionContent.isEncoded)
        return false;

    return true;
}

std::shared_ptr<DB2Ver3::WDC3Record> DB2Ver3::getRecord(const int recordIndex) {
    if (isSparse())
        return nullptr;

    int indexWithinSection = -1;
    int sectionIndex = -1;
    if (!getSectionIndex(recordIndex, sectionIndex, indexWithinSection))
        return nullptr;

    auto &sectionContent = sections[sectionIndex];
    auto &sectionHeader = section_headers[sectionIndex];

    uint32_t recordId = 0;
    if (sectionHeader.id_list_size > 0) {
        recordId = sectionContent.id_list[indexWithinSection];
    }
    if (recordId == 0) {
        //Some sections have id_list, but have 0 as an entry there for the record.
        //That's when we need calc recordId this way
        recordId = header->min_id+recordIndex;
    }

    unsigned char *recordPointer = sectionContent.records[indexWithinSection].data;

    return std::make_shared<DB2Ver3::WDC3Record>(shared_from_this(), recordId, recordIndex, recordPointer, sectionIndex);
}
std::shared_ptr<DB2Ver3::WDC3RecordSparse> DB2Ver3::getSparseRecord(int recordIndex) {
    if (!isSparse())
        return nullptr;

    int indexWithinSection = -1;
    int sectionIndex = -1;
    if (!getSectionIndex(recordIndex, sectionIndex, indexWithinSection))
        return nullptr;

    auto &sectionContent = sections[sectionIndex];
    auto &sectionHeader = section_headers[sectionIndex];

    int recordId = sectionContent.offset_map_id_list[indexWithinSection];
    unsigned char *recordPointer =
          sectionContent.variable_record_data
        + sectionContent.offset_map[indexWithinSection].offset
        - sectionHeader.file_offset;

    return std::make_shared<DB2Ver3::WDC3RecordSparse>(shared_from_this(), recordId, recordPointer);
}

int DB2Ver3::iterateOverCopyRecords(const std::function<void(int oldRecId, int newRecId)>& iterateFunction) {
    for (int i = 0; i < sections.size(); i++) {
        if (sections[i].isEncoded) continue;
//        if (section_headers[i].tact_key_hash != 0) continue;

        auto const &section = sections[i];
        for (int j = 0; j < section_headers[i].copy_table_count; j++) {
            auto &copyRecord = sections[i].copy_table[j];

            if (copyRecord.id_of_copied_row != copyRecord.id_of_new_row) {
                iterateFunction(sections[i].copy_table[j].id_of_copied_row, sections[i].copy_table[j].id_of_new_row);
            }
        }
    }
    return 0;
}
int DB2Ver3::getRelationRecord(int recordIndex) {
    int sectionIndex = 0;
    while (recordIndex >= section_headers[sectionIndex].record_count) {
        recordIndex -= section_headers[sectionIndex].record_count;
        sectionIndex++;
    }

    return getRelationRecord(recordIndex, sectionIndex);
}
int DB2Ver3::getRelationRecord(int recordIndexInSection, int sectionIndex) {
    int result = sections[sectionIndex].perRecordIndexRelation[recordIndexInSection];
    return result;
}

void DB2Ver3::guessFieldSizeForCommon(int fieldSizeBits, int &elementSizeBytes, int &arraySize) {
    if (fieldSizeBits == 64 || fieldSizeBits == 32 ||  fieldSizeBits == 16 || fieldSizeBits == 8) {
        arraySize = 1;
        elementSizeBytes = fieldSizeBits >> 3;
        return;
    }
    int fieldTotalSizeInBytes = fieldSizeBits >> 3;

    if (fieldTotalSizeInBytes % 4 == 0) {
        arraySize = fieldTotalSizeInBytes / 4;
        elementSizeBytes = 4;
    } else {
        arraySize = fieldTotalSizeInBytes;
        elementSizeBytes = 1;
    }
}

std::string DB2Ver3::getLayoutHash() {
    std::stringstream res;
    res << std::setfill('0') << std::setw(8) << std::hex << header->layout_hash ;
    std::string resStr = res.str();
    std::locale locale;
    auto to_upper = [&locale] (char ch) { return std::use_facet<std::ctype<char>>(locale).toupper(ch); };

    std::transform(resStr.begin(), resStr.end(), resStr.begin(), to_upper);
//    std::cout << "layoutHash = " << resStr << std::endl;
    return resStr;
}

int DB2Ver3::isSparse() { return header->flags.isSparse; }

const field_storage_info * const DB2Ver3::getFieldInfo(uint32_t fieldIndex) const {
    if (fieldIndex >= header->field_count) {
        std::cout << "fieldIndex = " << fieldIndex << " is bigger than field count = " << header->field_count << std::endl;
        return nullptr;
    }
    return &field_info[fieldIndex];
}

//---------------------------
//- DB Record classes
//---------------------------

DB2Ver3::WDC3Record::WDC3Record(std::shared_ptr<DB2Ver3 const> db2Class, int recordId, uint32_t recordIndex,
                                unsigned char *recordPointer, uint32_t sectionIndex) :
                                db2Class(db2Class), recordId(recordId), recordIndex(recordIndex),
                                recordPointer(recordPointer), sectionIndex(sectionIndex)
{

}

static inline void fixPaletteValue(WDC3::DB2Ver3::WDCFieldValue &value, int externalElemSizeBytes) {
    if (externalElemSizeBytes > 0 && externalElemSizeBytes < 4) {
        uint32_t mask = (1 << (externalElemSizeBytes*8)) - 1;
        value.v32 = value.v32 & mask;
    }
}

std::vector<WDC3::DB2Ver3::WDCFieldValue> DB2Ver3::WDC3Record::getField(int fieldIndex, int externalArraySize, int externalElemSizeBytes) const {
    auto const db2Header = db2Class->header;

    std::vector<WDC3::DB2Ver3::WDCFieldValue> result = {};

    auto const fieldInfo = db2Class->getFieldInfo(fieldIndex);
    if (fieldInfo == nullptr) {
        return {};
    }

    switch (fieldInfo->storage_type) {
        case field_compression::field_compression_none: {
            int byteOffset = fieldInfo->field_offset_bits >> 3;
            int bytesToRead = fieldInfo->field_size_bits >> 3;

            unsigned char *fieldDataPointer = &recordPointer[byteOffset];

            //Current return datatype uint64_t supports only up to 64bits
            int arraySize = 1;
            int fieldSize = 1;
            if (externalArraySize == -1) {
                db2Class->guessFieldSizeForCommon(fieldInfo->field_size_bits, fieldSize, arraySize);
            } else {
                arraySize = externalArraySize;
                fieldSize = externalElemSizeBytes;
            }

            for (int j = 0; j < arraySize; j++) {
                auto &fieldValue = result.emplace_back();


                static_assert(sizeof(fieldValue) == 8);
                std::memcpy(&fieldValue, &fieldDataPointer[fieldSize*j], fieldSize);
            }

            break;
        }


        case field_compression::field_compression_bitpacked:
        case field_compression::field_compression_bitpacked_signed: {
            uint32_t unpackedValue = 0;

            unsigned int bitOffset = fieldInfo->field_offset_bits;
            unsigned int bitesToRead = fieldInfo->field_size_bits;

            unpackedValue = get_bits(recordPointer, bitOffset, bitesToRead);

            auto &fieldValue = result.emplace_back();

            if (fieldInfo->storage_type == field_compression::field_compression_bitpacked_signed) {
                uint32_t value = unpackedValue;
                value = value << (32-bitesToRead);
                int32_t value_s = *(int32_t *) &value;
                value_s = value_s >> (32-bitesToRead);

                fieldValue.v32s = value_s;
            } else {
                fieldValue.v32 = unpackedValue;
            }

            break;
        }

        case field_compression::field_compression_common_data: {
            auto &fieldValue = result.emplace_back();

            fieldValue.v32 = fieldInfo->field_compression_common_data.default_value;
            //If id is found in commonData - take it from there instead of default value
            if (fieldInfo->additional_data_size > 0) {
                auto it = db2Class->commonDataHashMap[fieldIndex].find(recordId);
                if (it != db2Class->commonDataHashMap[fieldIndex].end()) {
                    fieldValue.v32 = it->second;
                }
            }
            break;
        }

        case field_compression::field_compression_bitpacked_indexed:
        case field_compression::field_compression_bitpacked_indexed_array: {

            unsigned int bitOffset = fieldInfo->field_compression_bitpacked_indexed.bitpacking_offset_bits;
            unsigned int bitesToRead = fieldInfo->field_compression_bitpacked_indexed.bitpacking_size_bits;

            bitOffset = fieldInfo->field_offset_bits;

            int palleteIndex = get_bits(recordPointer, bitOffset, bitesToRead);

            if (fieldInfo->storage_type == field_compression::field_compression_bitpacked_indexed_array) {
                int array_count = fieldInfo->field_compression_bitpacked_indexed_array.array_count;
                for (int j = 0; j < array_count; j++) {
                    int properPalleteIndex = (palleteIndex * array_count) + j;

                    //Safety check that doesnt happen as of 6/5/2022 on latest client
                    assert(properPalleteIndex < db2Class->palleteDataArray[fieldIndex].size());

                    auto &fieldValue = result.emplace_back();
                    fieldValue.v32 = db2Class->palleteDataArray[fieldIndex][properPalleteIndex];;
                    fixPaletteValue(fieldValue, externalElemSizeBytes);
                }
            } else {
                int properPalleteIndex = palleteIndex;
                assert(properPalleteIndex < db2Class->palleteDataArray[fieldIndex].size());

                auto &fieldValue = result.emplace_back();
                fieldValue.v32 = db2Class->palleteDataArray[fieldIndex][properPalleteIndex];
                fixPaletteValue(fieldValue, externalElemSizeBytes);
            }



            break;
        }
    }

    return result;
}

std::string DB2Ver3::WDC3Record::readString(int fieldIndex, int fieldElementOffset, int stringOffset) const {
    const wdc3_db2_header * const db2Header = db2Class->header;

    if (fieldIndex >= db2Header->field_count) {
        return "!#Invalid field number";
    }

    if (stringOffset == 0)
        return ""; //How else would you mark empty string? Only through null offset

    uint32_t fieldOffsetIntoGlobalArray =
            (recordIndex * db2Header->record_size) +
            (db2Class->field_info[fieldIndex].field_offset_bits >> 3) + fieldElementOffset;

    uint32_t stringOffsetIntoGlobalStringSection =
            (fieldOffsetIntoGlobalArray + stringOffset) - (db2Header->record_count*db2Header->record_size);

    int offset = stringOffsetIntoGlobalStringSection; // NOLINT(cppcoreguidelines-narrowing-conversions)

    assert(offset > 0);

    //Find section that is referenced by this offset
    const auto sectionHeaders = db2Class->section_headers;
    int sectionIndexforStr = 0;
    while (sectionIndexforStr < db2Class->sections.size() && offset >= sectionHeaders[sectionIndexforStr].string_table_size) {
        assert(offset >= sectionHeaders[sectionIndexforStr].string_table_size && offset >= 0);
        offset -= sectionHeaders[sectionIndexforStr].string_table_size;

        sectionIndexforStr++;
    }

    if (sectionIndexforStr >= db2Header->section_count) {
        return "!#Found invalid section for String, offset = " + std::to_string(offset);
    }

    if (offset < 0 || offset >= sectionHeaders[sectionIndexforStr].string_table_size) {
        return "!#Found invalid offset into Section's string table ";
    }

    std::string result = std::string((char *)&db2Class->sections[sectionIndexforStr].string_data[offset]);
/*    if (sectionIndexforStr != sectionIndex) {
        std::cout << "Enemy spotted: db2 = " << this->db2FileName
                  << " recordIndex = " << this->currentRecord
                  << " fieldIndex = " << this->currentField
                  << " expected section of string = " << sectionIndex
                  << " real section of string = " << sectionIdforStr
                  << " string content = " << result << std::endl;
    }*/

    return result;
}

/* ------------------------
 * WDC3RecordSparse
 * -------------------------
 */

DB2Ver3::WDC3RecordSparse::WDC3RecordSparse(const std::shared_ptr<const DB2Ver3> &db2Class, int recordId,
                                            unsigned char *recordPointer) :
                                            db2Class(db2Class), recordId(recordId), recordPointer(recordPointer) {

}
std::vector<WDC3::DB2Ver3::WDCFieldValue> DB2Ver3::WDC3RecordSparse::readNextField(int arrayElementSizeInBytes, int arraySize) {
    std::vector<WDC3::DB2Ver3::WDCFieldValue> result = {};


    if (arrayElementSizeInBytes <= 0) {
        guessFieldSizeForCommon(db2Class->getFieldInfo(currentFieldIndex)->field_size_bits,
                                arrayElementSizeInBytes, arraySize);
    }

    for (int i = 0; i < arraySize; i++) {
        auto &fieldValue = result.emplace_back();
        static_assert(sizeof (fieldValue) == 8);
        fieldValue.v64 = 0;

        std::memcpy(&fieldValue, &recordPointer[fieldOffset], arrayElementSizeInBytes);
        fieldOffset += arrayElementSizeInBytes;
    }
    currentFieldIndex++;

    return result;
}

std::string DB2Ver3::WDC3RecordSparse::readNextAsString() {
    std::string result = std::string((char *)&recordPointer[fieldOffset]);
    fieldOffset+=result.length()+1;

    if (fieldOffset > db2Class->header->record_size) {
        return "Reading a field resulted in buffer overflow";
    }

    currentFieldIndex++;
    return result;
}

