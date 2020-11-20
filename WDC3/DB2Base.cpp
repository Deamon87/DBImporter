//
// Created by deamon on 02.04.18.
//

#include <cstdint>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <assert.h>
#include <cmath>
#include <iomanip>
#include "DB2Base.h"

using namespace WDC3;

bool checkDataIfNonZero(unsigned char *ptr, int length) {
    for (int i = 0; i < length; i++) {
        if (ptr[i] != 0)
            return false;
    }

    return true;
}

void DB2Base::process(HFileContent db2File, const std::string &fileName) {
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
            if ((field_info[i].storage_type == field_compression_bitpacked_indexed) ||
                (field_info[i].storage_type == field_compression_bitpacked_indexed_array)) {

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
    bool prevSectionWasEncrypted = false;
    for (int i = 0; i < header->section_count; i++) {
        auto &itemSectionHeader = section_headers[i];
        sections.resize(sections.size()+1);
        section &section = sections[sections.size()-1];

//        if (itemSectionHeader.tact_key_hash != 0) break;
        //Prev section was encrypted. So it's required to fix current position in file
        if (prevSectionWasEncrypted) {
            currentOffset = itemSectionHeader.file_offset;
            prevSectionWasEncrypted = false;
        }

        assert(itemSectionHeader.file_offset == currentOffset);

        if ((header->flags & 1) == 0) {
            // Normal records

            for (int j = 0; j < itemSectionHeader.record_count; j++) {
                record_data recordData;
                readValues(recordData.data, header->record_size);

                section.records.push_back(recordData);
            }
            readValues(section.string_data, itemSectionHeader.string_table_size);

            //Check if section was decrypted properly
            if (itemSectionHeader.tact_key_hash != 0) {
                bool dataIsZero = true;
                bool stringsAreZero = true;
                if (section.records.size() > 0) {
                    dataIsZero = checkDataIfNonZero(section.records[0].data,
                                                    itemSectionHeader.record_count * header->record_size);
                }
                if (itemSectionHeader.string_table_size > 0) {
                    stringsAreZero = checkDataIfNonZero(section.string_data, itemSectionHeader.string_table_size);
                }

                //The section was not decrypted properly, so no point in parsing it further
                if (dataIsZero && stringsAreZero) {
                    prevSectionWasEncrypted = true;
                    section.isEncoded = true;
                    continue;
                }
            }

        } else {
            // Offset map records -- these records have null-terminated strings inlined, and
            // since they are variable-length, they are pointed to by an array of 6-byte
            // offset+size pairs.
            readValues(section.variable_record_data, itemSectionHeader.offset_records_end - itemSectionHeader.file_offset);
            bool dataIsZero = checkDataIfNonZero(section.variable_record_data, itemSectionHeader.offset_records_end - itemSectionHeader.file_offset);
            if (dataIsZero) {
                prevSectionWasEncrypted = true;
                section.isEncoded = true;
                continue;
            }
        }

        if (itemSectionHeader.offset_records_end > 0) {
            assert(itemSectionHeader.offset_records_end == currentOffset);
        }
//        if ((header->flags & 0x04) != 0) {
            readValues(section.id_list, itemSectionHeader.id_list_size / 4);
//        }
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

            for (int relationInd = 0; relationInd < section.relationship_map.num_entries; relationInd++) {
                const auto &entry = section.relationship_map.entries[relationInd];
                section.perRecordIndexRelation[entry.record_index] = entry.foreign_id;
            }

        }
        if (offsetBeforRelationshipData + itemSectionHeader.relationship_data_size != currentOffset) {
            currentOffset = offsetBeforRelationshipData + itemSectionHeader.relationship_data_size;
        }

        readValues(section.offset_map_id_list, itemSectionHeader.offset_map_id_count);
    }

    m_loaded = true;
}


std::string DB2Base::readString(unsigned char* &fieldPointer, int sectionIndex) {
    std::string result = "";
    if ((header->flags & 1) == 0) {
        int sizeToStringData = header->record_count*header->record_size;
        int32_t offset = fieldPointer + (*((uint32_t *)fieldPointer)) - sections[sectionIndex].records[0].data;

        //Get to the border of record data
        for (int i = 0; i < sectionIndex; i++) {
            sizeToStringData -= section_headers[i].record_count * header->record_size;
        }
        offset -= sizeToStringData;

        //Find section that is referenced by this offset
        int sectionIdforStr = 0;
        while (sectionIdforStr < sectionIndex && offset >= section_headers[sectionIdforStr].string_table_size) {
            offset -= section_headers[sectionIdforStr].string_table_size;
            sectionIdforStr++;
        }

        result = std::string((char *)&sections[sectionIdforStr].string_data[offset]);
        if (sectionIdforStr != sectionIndex) {
            std::cout << "Enemy spotted: db2 = " << this->db2FileName
                << " recordIndex = " << this->currentRecord
                << " fieldIndex = " << this->currentField
                << " expected section of string = " << sectionIndex
                << " real section of string = " << sectionIdforStr
                << " string content = " << result << std::endl;
        }
    } else {
        result = std::string((char *)fieldPointer);
        fieldPointer+=result.length()+1;
    }

    return result;
}


int get_bit(unsigned char *data, unsigned bitoffset) // returns the n-th bit
{
    int c = (int)(data[bitoffset >> 3]); // X>>3 is X/8
    int bitmask = 1 << (bitoffset & 7);  // X&7 is X%8
    return ((c & bitmask)!=0) ? 1 : 0;
}

unsigned int get_bits(unsigned char* data, unsigned bitOffset, unsigned numBits)
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

bool DB2Base::readRecordByIndex(int index, int minFieldNum, int fieldsToRead,
                                std::function<void(uint32_t &recordId, int fieldNum, int subIndex, int sectionNum, unsigned char * &data, size_t length)> callback) {
    //Find Record by section
    int sectionIndex = 0;
    int globalRecordIndex = index;
    while (index >= section_headers[sectionIndex].record_count) {
        index -= section_headers[sectionIndex].record_count;
        sectionIndex++;
        if (sectionIndex >= sections.size())
            return false;
    }

    this->currentRecord = index;


    auto &sectionDef = sections[sectionIndex];
    auto &sectionHeader = section_headers[sectionIndex];
    //
    if (sectionDef.isEncoded) return false;

    int numOfFieldToRead = fieldsToRead >=0 ? fieldsToRead : header->field_count;
    uint32_t recordId = 0;


    uint8_t * recordPointer = nullptr;
    if ((header->flags & 1) == 0) {
        if (sectionHeader.id_list_size > 0) {
            recordId = sectionDef.id_list[index];
        }
        if (recordId == 0) {
            recordId = header->min_id+globalRecordIndex;
        }
        recordPointer = sectionDef.records[index].data;
    } else {
        recordId = sectionDef.offset_map_id_list[index];
//        if (sectionDef.offset_map[index].size == 0) {
//            return false;
//        }

        recordPointer = sectionDef.variable_record_data+sectionDef.offset_map[index].offset - sectionHeader.file_offset;
//        sectionDef.variable_record_data
    }

    unsigned char *fieldDataPointer = &recordPointer[0];
    for (int i = minFieldNum; i < numOfFieldToRead; i++) {
        this->currentField = i;

        if ((header->flags & 1) == 0) {
            auto &fieldInfo = field_info[i];

            switch (fieldInfo.storage_type) {
                case field_compression_none: {
                    int byteOffset = fieldInfo.field_offset_bits >> 3;
                    int bytesToRead = fieldInfo.field_size_bits >> 3;

                    unsigned char *fieldDataPointer = &recordPointer[byteOffset];

                    callback(recordId, i, -1, sectionIndex, fieldDataPointer, bytesToRead);
                }
                break;


                case field_compression_bitpacked:
                case field_compression_bitpacked_signed:
                {
                    uint8_t buffer[128];

                    unsigned int bitOffset = fieldInfo.field_offset_bits;
                    unsigned int bitesToRead = fieldInfo.field_size_bits;

                    //Zero the buffer
                    for (int j = 0; j < 128; j++) buffer[j] = 0;

                    *((uint32_t*) &buffer[0]) = get_bits(recordPointer, bitOffset, bitesToRead);

                    if (fieldInfo.storage_type == field_compression_bitpacked_signed) {
                        uint32_t value = *((uint32_t *) &buffer[0]);
                        value = value << (32-bitesToRead);
                        int32_t value_s = *(int32_t *) &value;
                        value_s = value_s >> (32-bitesToRead);
                        *((int32_t*) &buffer[0]) = value_s;
                    }


                    unsigned char *fieldDataPointer = &buffer[0];
                    callback(recordId, i, -1, sectionIndex, fieldDataPointer, bitesToRead >> 3);
                }
                break;
                case field_compression_common_data: {
                    uint32_t value = fieldInfo.field_compression_common_data.default_value;
                    //If id is found in commonData - take it from there instead of default value
                    if (fieldInfo.additional_data_size > 0) {
                        auto it = commonDataHashMap[i].find(recordId);
                        if (it != commonDataHashMap[i].end()) {
                            value = it->second;
                        }

                        size_t bytesToRead = fieldInfo.field_size_bits >> 3;
                        uint8_t *ptr = (uint8_t *) &value;

                        callback(recordId, i, -1, sectionIndex, ptr, bytesToRead);
                    }
                }
                break;
                case field_compression_bitpacked_indexed:
                case field_compression_bitpacked_indexed_array:
                    uint8_t buffer[128];

                    unsigned int bitOffset = fieldInfo.field_compression_bitpacked_indexed.bitpacking_offset_bits;
                    unsigned int bitesToRead = fieldInfo.field_compression_bitpacked_indexed.bitpacking_size_bits;

                    bitOffset = fieldInfo.field_offset_bits;

                    //Zero the buffer
                    for (int j = 0; j < 128; j++) buffer[j] = 0;

                    *((uint32_t*) &buffer[0]) = get_bits(recordPointer, bitOffset, bitesToRead);
                    int palleteIndex = *(uint32_t *)&buffer[0];

//                    uint8_t *ptr = reinterpret_cast<uint8_t *>(&pallet_data[properIndexForPalleteData + (palleteIndex*4)]);
                    if (fieldInfo.storage_type == field_compression_bitpacked_indexed_array) {
                        int array_count = fieldInfo.field_compression_bitpacked_indexed_array.array_count;
                        for (int j = 0; j < array_count; j++) {
                            int properPalleteIndex = (palleteIndex*array_count)+j;
                            if (properPalleteIndex >= palleteDataArray[i].size()) {
                                properPalleteIndex = 0; // TODO: HACK
                            }
                            uint32_t value = palleteDataArray[i][properPalleteIndex];
                            uint8_t *ptr = (uint8_t *) &value;

                            callback(recordId, i, j, sectionIndex, ptr, 4);
                        }
                    } else {
                        int properPalleteIndex = palleteIndex;
                        if (properPalleteIndex >= palleteDataArray[i].size()) {
                            properPalleteIndex = 0; // TODO: HACK
                        }
                        uint8_t *ptr = reinterpret_cast<uint8_t *>(&palleteDataArray[i][properPalleteIndex]);

                        callback(recordId, i, -1, sectionIndex, ptr, 4);
                    }

                    break;
            }

        } else {
            //variable data
            auto &fieldInfo = field_info[i];
            int bytesToRead = fieldInfo.field_size_bits >> 3;



            callback(recordId, i, -1, sectionIndex, fieldDataPointer, bytesToRead);
        }
    }


    return true;
}

int DB2Base::iterateOverCopyRecords(const std::function<void(int oldRecId, int newRecId)>& iterateFunction) {
    for (int i = 0; i < sections.size(); i++) {
        if (sections[i].isEncoded) return false;
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
int DB2Base::getRelationRecord(int recordIndex) {
    int sectionIndex = 0;
    while (recordIndex >= section_headers[sectionIndex].record_count) {
        recordIndex -= section_headers[sectionIndex].record_count;
        sectionIndex++;
    }

    return getRelationRecord(recordIndex, sectionIndex);
}
int DB2Base::getRelationRecord(int recordIndexInSection, int sectionIndex) {
    int result = sections[sectionIndex].perRecordIndexRelation[recordIndexInSection];
    return result;
}

std::string DB2Base::getLayoutHash() {
    std::stringstream res;
    res << std::setfill('0') << std::setw(8) << std::hex << header->layout_hash ;
    std::string resStr = res.str();
    std::locale locale;
    auto to_upper = [&locale] (char ch) { return std::use_facet<std::ctype<char>>(locale).toupper(ch); };

    std::transform(resStr.begin(), resStr.end(), resStr.begin(), to_upper);
    std::cout << "laoutHash = " << resStr << std::endl;
    return resStr;
}
