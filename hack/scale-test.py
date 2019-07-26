from hailtop import pipeline
import sys

N = int(sys.argv[1])
M = int(sys.argv[2])
print(f'N {N} M {M}')

p = pipeline.Pipeline(backend=pipeline.HackBackend(), default_image='ubuntu:18.04')

n = []
for i in range(N):
    t = p.new_task(f'N{i}')
    t.command(f'head -c 10 </dev/random | base64 >{t.ofile}')
    n.append(t)

m = []
for j in range(M):
    t = p.new_task(f'M{j}')
    t.command(f'head -c 10 </dev/random | base64 >{t.ofile}')
    m.append(t)

for i in range(N):
    for j in range(M):
        t = p.new_task(f'X{i},{j}')
        a = n[i]
        b = m[j]
        t.command(f'cat {a.ofile} {b.ofile} | sum > {t.sum}')

        p.write_output(t.sum, 'gs://hail-cseed/cs-hack/x/sum_{i}_{j}.txt')

p.run()
