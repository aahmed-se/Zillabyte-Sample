import zillabyte

domains = []


def prep():
    global domains
    with open('list.txt') as f:
        domains = [line.rstrip() for line in f]
    return


prep()

# The begin_function is where you initialize any state
# In this case, we save the ["domain", "url"] pair we wish to count
def domain_count_begin_group(controller, g_tuple):
    global domain_word
    global domain_count
    domain_word = g_tuple["domain"]
    domain_count = 0


# In this simple case, increment the counter
def domain_count_aggregate_group(controller, tup):
    global domain_count
    domain_count += 1


# This is run after all tuples have been received for the cycle
# We emit the "word" , "url", and the count of the pair in tuples
def domain_count_end_group(controller):
    global domain_word
    global domain_count
    controller.emit({"domain": domain_word, "count": domain_count})


# This is the heart of your algorithm.  It's processed on every
# web page.  This algorithm is run in parallel on possibly hundreds
# of machines.
def domain_count(controller, tup):
    for domain in domains:
        if (domain in tup["html"]):
            controller.emit({"domain": domain})


app = zillabyte.app(name="hello_world")
app.source(matches="sample_homepages") \
    .each(execute=domain_count) \
    .group_by( \
    name="domain_count", \
    fields=["domain"], \
    begin_group=domain_count_begin_group, \
    aggregate=domain_count_aggregate_group, \
    end_group=domain_count_end_group \
    )\
	.sink(name="domain_names", columns=[{"domain": "string"}, {"count": "integer"}])
