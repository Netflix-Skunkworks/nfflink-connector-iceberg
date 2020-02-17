package com.netflix.spaas.nfflink.connector.iceberg.model;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StudentUtil {

    public static boolean isEqual(Map<String, Object> studentMap, GenericRecord genericRecord, Schema schema) {
        if (!studentMap.get("id").equals(genericRecord.get("id")))
            return false;
        if (!studentMap.get("age").equals(genericRecord.get("age")))
            return false;
        if (!studentMap.get("married").equals(genericRecord.get("married")))
            return false;
        if (!studentMap.get("average").equals(genericRecord.get("average")))
            return false;
        if (!studentMap.get("max").equals(genericRecord.get("max")))
            return false;
        if (!studentMap.get("comment").toString().equals(genericRecord.get("comment").toString()))
            return false;
        if (!ByteBuffer.wrap((byte[]) studentMap.get("crc32")).equals(genericRecord.get("crc32")))
            return false;
        if (!(new GenericData.Fixed(schema.getField(
                "signature").schema(), (byte[]) studentMap.get("signature")))
                .equals(genericRecord.get("signature")))
            return false;
        if (!studentMap.get("suit").toString().equals(genericRecord.get("suit").toString()))
            return false;
        if (!studentMap.get("scores").equals(genericRecord.get("scores")))
            return false;
        if (!isFriendsEqual((List<Map<CharSequence, CharSequence>>) studentMap.get("friends"), (List<Map<CharSequence, CharSequence>>) genericRecord.get("friends")))
            return false;
        if (!isCoursesMapEqual((List<Map<CharSequence, Object>>) studentMap.get("courses"), (List<GenericRecord>) genericRecord.get("courses"))) {
            return false;
        }
        if (!isContactEqual((Map<String, Object>) studentMap.get("contact"), (GenericRecord) genericRecord.get("contact")))
            return false;
        if (!isCharSeqMapEqual((Map<CharSequence, CharSequence>) studentMap.get("tags"), (Map<CharSequence, CharSequence>) genericRecord.get("tags")))
            return false;
        if (!(studentMap.get("classes") == null && genericRecord.get("classes") == null) &&
                !(isCharSeqListEqual((List<CharSequence>) studentMap.get("classes"), (List<CharSequence>) genericRecord.get("classes"))))
            return false;
        if (!(studentMap.get("alias") == null && genericRecord.get("alias") == null) &&
                !(studentMap.get("alias").toString().equals(genericRecord.get("alias").toString())))
            return false;
        return true;
    }

    public static boolean isEqual(Student student, GenericRecord genericRecord) {
        if (!student.getId().equals(genericRecord.get("id")))
            return false;
        if (!student.getAge().equals(genericRecord.get("age")))
            return false;
        if (!student.getMarried().equals(genericRecord.get("married")))
            return false;
        if (!student.getAverage().equals(genericRecord.get("average")))
            return false;
        if (!student.getMax().equals(genericRecord.get("max")))
            return false;
        if (!student.getComment().toString().equals(genericRecord.get("comment").toString()))
            return false;
        if (!student.getCrc32().equals(genericRecord.get("crc32")))
            return false;
        if (!student.getSignature().equals(genericRecord.get("signature")))
            return false;
        if (!student.getSuit().toString().equals(genericRecord.get("suit").toString()))
            return false;
        if (!student.getScores().equals(genericRecord.get("scores")))
            return false;
        if (!isFriendsEqual(student.getFriends(), (List<Map<CharSequence, CharSequence>>) genericRecord.get("friends")))
            return false;
        if (!isCoursesEqual(student.getCourses(), (List<GenericRecord>) genericRecord.get("courses")))
            return false;
        if (!isContactEqual(student.getContact(), (GenericRecord) genericRecord.get("contact")))
            return false;
        if (!isCharSeqMapEqual(student.getTags(), (Map<CharSequence, CharSequence>) genericRecord.get("tags")))
            return false;
        if (!(student.getClasses() == null && genericRecord.get("classes") == null) &&
                !(isCharSeqListEqual(student.getClasses(), (List<CharSequence>) genericRecord.get("classes"))))
            return false;
        if (!(student.getAlias() == null && genericRecord.get("alias") == null) &&
                !(student.getAlias().toString().equals(genericRecord.get("alias").toString())))
            return false;
        return true;
    }

    private static boolean isCoursesMapEqual(List<Map<CharSequence, Object>> courses, List<GenericRecord> genericList) {
        if (courses.size() != genericList.size())
            return false;
        for (int i = 0 ; i < courses.size(); ++i) {
            Map<CharSequence, Object> course = courses.get(i);
            GenericRecord record = genericList.get(i);
            if (!course.get("name").toString().equals(record.get("name").toString()))
                return false;
            if (!course.get("id").equals(record.get("id")))
                return false;
        }
        return true;
    }

    private static boolean isCoursesEqual(List<Course> courses, List<GenericRecord> genericList) {
        if (courses.size() != genericList.size())
            return false;
        for (int i = 0 ; i < courses.size(); ++i) {
            Course course = courses.get(i);
            GenericRecord record = genericList.get(i);
            if (!course.getName().toString().equals(record.get("name").toString()))
                return false;
            if (!course.getId().equals(record.get("id")))
                return false;
        }
        return true;
    }

    private static boolean isFriendsEqual(List<Map<CharSequence, CharSequence>> friends, List<Map<CharSequence, CharSequence>> genericList) {
        if (friends.size() != genericList.size())
            return false;
        for (int i = 0 ; i < friends.size(); ++i) {
            if (!isCharSeqMapEqual(friends.get(i), genericList.get(i)))
                return false;
        }
        return true;
    }

    private static boolean isContactEqual(Contact contact, GenericRecord genericRecord) {
        if (!contact.getAddress().toString().equals(genericRecord.get("address").toString()))
            return false;
        if (!contact.getPhone().toString().equals(genericRecord.get("phone").toString()))
            return false;
        return true;
    }

    private static boolean isContactEqual(Map<String, Object> contact, GenericRecord genericRecord) {
        if (!contact.get("address").toString().equals(genericRecord.get("address").toString()))
            return false;
        if (!contact.get("phone").toString().equals(genericRecord.get("phone").toString()))
            return false;
        return true;
    }

    private static boolean isCharSeqMapEqual(Map<CharSequence, CharSequence> tags, Map<CharSequence, CharSequence> genericMap) {
        Map<String, String> strTags = tags.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toString()
                ));
        Map<String, String> strGenericMap = genericMap.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toString()
                ));
        return strTags.equals(strGenericMap);
    }

    private static boolean isCharSeqListEqual(List<CharSequence> left, List<CharSequence> right) {
        List<String> leftStrs = left.stream().map(e -> null == e ? null : e.toString()).collect(Collectors.toList());
        List<String> rightStrs = left.stream().map(e -> null == e ? null : e.toString()).collect(Collectors.toList());
        return leftStrs.equals(rightStrs);
    }

    public static Map<String, Object> createStudentMap() {
        byte[] crc32Bytes = new byte[4];
        for (int i = 0; i < 4; ++i) {
            crc32Bytes[i] = (byte) i;
        }

        byte[] md5Bytes = new byte[16];
        for (int i = 0; i < 16; ++i) {
            md5Bytes[i] =  (byte) i;
        }

        Map<String, Object> studentMap = ImmutableMap.<String, Object>builder()
                .put("id", 1L)
                .put("age", 21)
                .put("married", false)
                .put("average", 3.6f)
                .put("max", 4.0)
                .put("comment", "testing")
                .put("crc32", crc32Bytes)
                .put("signature", md5Bytes)
                .put("suit", "CLUBS")
                .put("scores", Arrays.asList(1, 2, 3))
                .put("friends", Arrays.asList(
                        ImmutableMap.of("first_name", "joe", "last_name", "doe"),
                        ImmutableMap.of("first_name", "jane", "last_name", "doe")
                        )
                )
                .put("courses", Arrays.asList(
                        ImmutableMap.of("name", "cs101", "id", 1),
                        ImmutableMap.of("name", "cs202", "id", 2)
                ))
                .put("contact", ImmutableMap.<String, String>builder()
                        .put("address", "123 foo ave")
                        .put("phone", "123-456-7890")
                        .build())
                .put("tags", ImmutableMap.<String, String>builder()
                        .put("k1", "v1")
                        .put("k2", "v2")
                        .build())
                .build();
        return studentMap;
    }

    public static Student createStudent() {

        Student student = Student.newBuilder()
                .setId(1)
                .setAge(21)
                .setMarried(false)
                .setAverage(3.6f)
                .setMax(4.0)
                .setComment("testing")
                .setCrc32(createCRC32())
                .setSignature(createMD5())
                .setSuit(Suit.CLUBS)
                .setScores(Arrays.asList(1, 2, 3))
                .setFriends(Arrays.asList(
                        ImmutableMap.of("first_name", "joe", "last_name", "doe"),
                        ImmutableMap.of("first_name", "jane", "last_name", "doe")
                ))
                .setCourses(Arrays.asList(
                        Course.newBuilder().setName("cs101").setId(1).build(),
                        Course.newBuilder().setName("cs202").setId(2).build()
                ))
                .setContact(Contact.newBuilder()
                        .setAddress("123 foo ave")
                        .setPhone("123-456-7890")
                        .build())
                .setTags(ImmutableMap.<CharSequence, CharSequence>builder()
                        .put("k1", "v1")
                        .put("k2", "v2")
                        .build())
                .build();
        return student;
    }

    private static MD5 createMD5() {
        byte[] md5Bytes = new byte[16];
        for (int i = 0; i < 16; ++i) {
            md5Bytes[i] =  (byte) i;
        }
        return new MD5(md5Bytes);
    }

    private static ByteBuffer createCRC32() {
        ByteBuffer bb = ByteBuffer.allocate(4);
        for (int i = 0; i < 4; ++i) {
            bb.put((byte) i);
        }
        return bb;
    }


}
